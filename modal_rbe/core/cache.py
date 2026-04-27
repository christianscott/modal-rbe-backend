"""Parametrized CAS / AC layer.

A `CacheStore` bundles every storage handle the gRPC servicers and executor
need (cas dict, ac dict, cas volume, the volume-side Modal Functions) and
exposes the same async API the original module-level helpers did:

    cache.find_missing(hashes)
    cache.read(hash)
    cache.write(hash, size, blob)
    cache.batch_read(hashes)
    cache.batch_update(blobs)
    cache.get_action_result(hash)
    cache.put_action_result(hash, blob)

Callers create one `CacheStore` per deployment via `make_cache_store(...)`
which both builds the in-process LRU mirrors AND registers the volume-side
Modal Functions on the caller's `modal.App`.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import modal

from .digest import cas_path, ensure_parent, sha256_bytes
from .lru import BoundedLruDict, BoundedLruSet


# Tag bytes prefixed onto every cas_dict value. `\x00 + bytes` = inline,
# `\x01` = "look on the volume". Keeps cas_dict the authoritative presence
# map even for blobs whose bytes live on the volume.
_INLINE = b"\x00"
_VOLUME_MARKER = b"\x01"

# SHA-256 of the empty string. Bazel references it for empty stdout/stderr but
# never uploads it, so we have to treat it as always-present.
EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


@dataclass
class _VolumeHelpers:
    """References to the volume-side Modal Functions registered for a given
    cache. Filled in by `make_cache_store`."""

    write: modal.Function
    read: modal.Function
    batch_read: modal.Function
    batch_update: modal.Function


def _atomic_write(path: str, data: bytes) -> None:
    ensure_parent(path)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def _read_with_retry(path: str, volume: modal.Volume) -> Optional[bytes]:
    """Optimistic read; only call `volume.reload()` if the file is missing."""
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        volume.reload()
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None


def _decode(val: Optional[bytes]) -> tuple[str, Optional[bytes]]:
    """Decode a cas_dict value -> (state, inline_bytes).
    state ∈ {"missing", "inline", "volume"}.
    """
    if val is None or not val:
        return ("missing", None)
    tag = val[:1]
    if tag == _INLINE:
        return ("inline", val[1:])
    if tag == _VOLUME_MARKER:
        return ("volume", None)
    return ("missing", None)


@dataclass
class CacheStore:
    """The parametrized cache. Construct with `make_cache_store(...)`."""

    cas_volume: modal.Volume
    cas_dict: modal.Dict
    ac_dict: modal.Dict
    cas_mount: str = "/cas"
    small_threshold: int = 32 * 1024 * 1024
    modal_fanout_limit: int = 64
    cas_index_maxsize: int = 100_000
    ac_cache_maxsize: int = 50_000

    _volume: _VolumeHelpers = field(default=None)  # type: ignore[assignment]
    _known_keys: BoundedLruSet = field(init=False)
    _ac_cache: BoundedLruDict = field(init=False)
    _fanout_sem: Optional[asyncio.Semaphore] = field(default=None, init=False)

    def __post_init__(self):
        self._known_keys = BoundedLruSet(self.cas_index_maxsize)
        self._ac_cache = BoundedLruDict(self.ac_cache_maxsize)

    # ------------------------------------------------------------------
    # Internal: gate concurrent Modal-client calls so we don't blow up
    # the grpclib transport.
    # ------------------------------------------------------------------

    def _sem(self) -> asyncio.Semaphore:
        if self._fanout_sem is None:
            self._fanout_sem = asyncio.Semaphore(self.modal_fanout_limit)
        return self._fanout_sem

    async def _gated(self, coro):
        async with self._sem():
            return await coro

    # ------------------------------------------------------------------
    # CAS public API
    # ------------------------------------------------------------------

    async def find_missing(self, hashes: list[str]) -> list[str]:
        if not hashes:
            return []
        to_check = [
            h for h in hashes if h != EMPTY_HASH and h not in self._known_keys
        ]
        if not to_check:
            return []
        present = await asyncio.gather(
            *(self._gated(self.cas_dict.contains.aio(h)) for h in to_check)
        )
        missing: list[str] = []
        for h, p in zip(to_check, present):
            if p:
                self._known_keys.add(h)
            else:
                missing.append(h)
        return missing

    async def read(self, hash_: str) -> Optional[bytes]:
        if hash_ == EMPTY_HASH:
            return b""
        val = await self._gated(self.cas_dict.get.aio(hash_, None))
        state, inline = _decode(val)
        if state == "inline":
            self._known_keys.add(hash_)
            return inline
        if state == "volume":
            self._known_keys.add(hash_)
            return await self._gated(self._volume.read.remote.aio(hash_))
        return None

    async def write(self, hash_: str, size: int, blob: bytes) -> None:
        if len(blob) != size:
            raise ValueError(f"size mismatch: declared {size}, got {len(blob)}")
        actual = sha256_bytes(blob)
        if actual != hash_:
            raise ValueError(f"digest mismatch: declared {hash_}, got {actual}")
        if size <= self.small_threshold:
            await self._gated(self.cas_dict.put.aio(hash_, _INLINE + blob))
        else:
            # Bytes go on the volume first; only set the marker once the
            # volume write has succeeded so a marker always implies bytes
            # are present.
            await self._gated(self._volume.write.remote.aio(hash_, size, blob))
            await self._gated(self.cas_dict.put.aio(hash_, _VOLUME_MARKER))
        self._known_keys.add(hash_)

    async def batch_read(self, hashes: list[str]) -> list[tuple[str, Optional[bytes]]]:
        if not hashes:
            return []
        out: list[tuple[str, Optional[bytes]]] = []
        to_fetch: list[str] = []
        for h in hashes:
            if h == EMPTY_HASH:
                out.append((h, b""))
            else:
                to_fetch.append(h)
        if not to_fetch:
            return out
        vals = await asyncio.gather(
            *(self._gated(self.cas_dict.get.aio(h, None)) for h in to_fetch)
        )
        volume_hashes: list[str] = []
        for h, v in zip(to_fetch, vals):
            state, inline = _decode(v)
            if state == "inline":
                self._known_keys.add(h)
                out.append((h, inline))
            elif state == "volume":
                self._known_keys.add(h)
                volume_hashes.append(h)
            else:
                out.append((h, None))
        if volume_hashes:
            for h, b in await self._volume.batch_read.remote.aio(volume_hashes):
                out.append((h, b))
        return out

    async def batch_update(
        self, blobs: list[tuple[str, int, bytes]]
    ) -> list[tuple[str, str]]:
        results: list[tuple[str, str]] = []
        small: dict[str, bytes] = {}
        large: list[tuple[str, int, bytes]] = []
        for h, sz, b in blobs:
            try:
                if len(b) != sz:
                    raise ValueError(f"size mismatch: declared {sz}, got {len(b)}")
                actual = sha256_bytes(b)
                if actual != h:
                    raise ValueError(f"digest mismatch: declared {h}, got {actual}")
            except Exception as e:  # noqa: BLE001
                results.append((h, str(e)))
                continue
            if sz <= self.small_threshold:
                small[h] = _INLINE + b
            else:
                large.append((h, sz, b))
        if small:
            await self.cas_dict.update.aio(small)
            for h in small:
                self._known_keys.add(h)
            results.extend((h, "") for h in small)
        if large:
            large_results = await self._volume.batch_update.remote.aio(large)
            markers = {h: _VOLUME_MARKER for h, err in large_results if not err}
            if markers:
                await self.cas_dict.update.aio(markers)
                for h in markers:
                    self._known_keys.add(h)
            results.extend(large_results)
        return results

    # ------------------------------------------------------------------
    # AC public API
    # ------------------------------------------------------------------

    async def get_action_result(self, hash_: str) -> Optional[bytes]:
        cached = self._ac_cache.get(hash_)
        if cached is not None:
            return cached
        blob = await self.ac_dict.get.aio(hash_, None)
        if blob is not None:
            self._ac_cache.put(hash_, blob)
        return blob

    async def put_action_result(self, hash_: str, blob: bytes) -> None:
        await self.ac_dict.put.aio(hash_, blob)
        self._ac_cache.put(hash_, blob)


# ----------------------------------------------------------------------
# Factory: build a CacheStore + register volume-side Modal Functions on
# the caller's App.
# ----------------------------------------------------------------------


def make_cache_store(
    *,
    app: modal.App,
    cache_image: modal.Image,
    cas_volume: modal.Volume,
    cas_dict: modal.Dict,
    ac_dict: modal.Dict,
    cas_mount: str = "/cas",
    region: Optional[str] = None,
    volume_max_containers: int = 32,
    name_prefix: str = "_volume",
) -> CacheStore:
    """Build a `CacheStore` and register the volume-side Modal Functions
    (read / write / batch_read / batch_update) on `app`. The user calls this
    once near the top of their app.py and passes the resulting store to
    every executor pool and the gRPC server."""

    fn_kwargs: dict = {
        "image": cache_image,
        "volumes": {cas_mount: cas_volume},
        "max_containers": volume_max_containers,
        # The volume-side functions are defined as closures inside this
        # factory (they capture `cas_volume` and `cas_mount`). Modal
        # requires `serialized=True` for non-global-scope functions.
        "serialized": True,
    }
    if region is not None:
        fn_kwargs["region"] = region

    @app.function(**fn_kwargs, name=f"{name_prefix}_write")
    def _volume_write(hash_: str, size: int, blob: bytes) -> None:
        path = cas_path(cas_mount, hash_)
        if os.path.exists(path):
            return
        _atomic_write(path, blob)
        cas_volume.commit()

    @app.function(**fn_kwargs, name=f"{name_prefix}_read")
    def _volume_read(hash_: str) -> Optional[bytes]:
        return _read_with_retry(cas_path(cas_mount, hash_), cas_volume)

    @app.function(**fn_kwargs, name=f"{name_prefix}_batch_read")
    def _volume_batch_read(hashes: list[str]) -> list[tuple[str, Optional[bytes]]]:
        out: list[tuple[str, Optional[bytes]]] = []
        reloaded = False
        for h in hashes:
            path = cas_path(cas_mount, h)
            try:
                with open(path, "rb") as f:
                    out.append((h, f.read()))
            except FileNotFoundError:
                if not reloaded:
                    cas_volume.reload()
                    reloaded = True
                    try:
                        with open(path, "rb") as f:
                            out.append((h, f.read()))
                        continue
                    except FileNotFoundError:
                        pass
                out.append((h, None))
        return out

    @app.function(**fn_kwargs, name=f"{name_prefix}_batch_update")
    def _volume_batch_update(
        blobs: list[tuple[str, int, bytes]],
    ) -> list[tuple[str, str]]:
        results: list[tuple[str, str]] = []
        wrote = False
        for h, _sz, b in blobs:
            path = cas_path(cas_mount, h)
            try:
                if not os.path.exists(path):
                    _atomic_write(path, b)
                    wrote = True
                results.append((h, ""))
            except Exception as e:  # noqa: BLE001
                results.append((h, str(e)))
        if wrote:
            cas_volume.commit()
        return results

    store = CacheStore(
        cas_volume=cas_volume,
        cas_dict=cas_dict,
        ac_dict=ac_dict,
        cas_mount=cas_mount,
    )
    store._volume = _VolumeHelpers(
        write=_volume_write,
        read=_volume_read,
        batch_read=_volume_batch_read,
        batch_update=_volume_batch_update,
    )
    return store
