"""CAS storage layer with a hybrid Dict + Volume backend.

- Every blob has a `cas_dict` entry. Two encodings:
    * `b"\\x00" + bytes`   inline storage (used when size ≤ SMALL_THRESHOLD)
    * `b"\\x01"`           presence marker; bytes live on `cas_volume`
- Larger blobs live on `cas_volume` (eventually consistent, reload/commit).

The `cas_dict` is the authoritative presence map: `find_missing` only looks at
the Dict, never at the Volume. That eliminates Modal Function RTT from the
hottest RPC Bazel sends. Reads dispatch by inspecting the tag byte; writes
always update the Dict last so a present-marker implies the bytes are
already on the volume.

The public coroutines (`find_missing`, `read`, `write`, `batch_read`,
`batch_update`) are awaited by async gRPC servicers. Within a batch they fan
out per-hash dict ops via `asyncio.gather` so a single Bazel RPC pulls/pushes
many blobs in parallel rather than serially.
"""

from __future__ import annotations

import asyncio
import os

from .app import CAS_MOUNT, app, cache_image, cas_dict, cas_volume
from .digest import cas_path, ensure_parent, sha256_bytes

SMALL_THRESHOLD = 32 * 1024 * 1024  # blobs ≤ 32 MiB live inline in the Dict

# SHA-256 of the empty string. Bazel references it for empty stdout/stderr but
# never uploads it, so we have to treat it as always-present.
EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# Tag bytes prefixed onto every cas_dict value to disambiguate inline bytes
# from a "look on the volume" marker.
_INLINE = b"\x00"
_VOLUME_MARKER = b"\x01"

_MODAL_FANOUT = 64
_fanout_sem: asyncio.Semaphore | None = None


def _sem() -> asyncio.Semaphore:
    global _fanout_sem
    if _fanout_sem is None:
        _fanout_sem = asyncio.Semaphore(_MODAL_FANOUT)
    return _fanout_sem


async def _gated(coro):
    async with _sem():
        return await coro


def _decode(val: bytes | None) -> tuple[str, bytes | None]:
    """Decode a cas_dict value. Returns (state, inline_bytes).

    state ∈ {"missing", "inline", "volume"}.
    """
    if val is None:
        return ("missing", None)
    if not val:  # zero-length entry — defensive, treat as missing
        return ("missing", None)
    tag = val[:1]
    if tag == _INLINE:
        return ("inline", val[1:])
    if tag == _VOLUME_MARKER:
        return ("volume", None)
    return ("missing", None)


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------


async def find_missing(hashes: list[str]) -> list[str]:
    if not hashes:
        return []
    to_check = [h for h in hashes if h != EMPTY_HASH]
    if not to_check:
        return []
    present = await asyncio.gather(
        *(_gated(cas_dict.contains.aio(h)) for h in to_check)
    )
    return [h for h, p in zip(to_check, present) if not p]


async def read(hash_: str) -> bytes | None:
    if hash_ == EMPTY_HASH:
        return b""
    val = await _gated(cas_dict.get.aio(hash_, None))
    state, inline = _decode(val)
    if state == "inline":
        return inline
    if state == "volume":
        return await _gated(_volume_read.remote.aio(hash_))
    return None


async def write(hash_: str, size: int, blob: bytes) -> None:
    if len(blob) != size:
        raise ValueError(f"size mismatch: declared {size}, got {len(blob)}")
    actual = sha256_bytes(blob)
    if actual != hash_:
        raise ValueError(f"digest mismatch: declared {hash_}, got {actual}")
    if size <= SMALL_THRESHOLD:
        await _gated(cas_dict.put.aio(hash_, _INLINE + blob))
    else:
        # Bytes go on the volume first; only set the marker once the volume
        # write has succeeded so a marker always implies bytes are present.
        await _gated(_volume_write.remote.aio(hash_, size, blob))
        await _gated(cas_dict.put.aio(hash_, _VOLUME_MARKER))


async def batch_read(hashes: list[str]) -> list[tuple[str, bytes | None]]:
    if not hashes:
        return []
    out: list[tuple[str, bytes | None]] = []
    to_fetch: list[str] = []
    for h in hashes:
        if h == EMPTY_HASH:
            out.append((h, b""))
        else:
            to_fetch.append(h)
    if not to_fetch:
        return out
    vals = await asyncio.gather(
        *(_gated(cas_dict.get.aio(h, None)) for h in to_fetch)
    )
    volume_hashes: list[str] = []
    for h, v in zip(to_fetch, vals):
        state, inline = _decode(v)
        if state == "inline":
            out.append((h, inline))
        elif state == "volume":
            volume_hashes.append(h)
        else:
            out.append((h, None))
    if volume_hashes:
        for h, b in await _volume_batch_read.remote.aio(volume_hashes):
            out.append((h, b))
    return out


async def batch_update(blobs: list[tuple[str, int, bytes]]) -> list[tuple[str, str]]:
    """Returns (hash, error_message); empty error means success."""
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
        if sz <= SMALL_THRESHOLD:
            small[h] = _INLINE + b
        else:
            large.append((h, sz, b))
    if small:
        await cas_dict.update.aio(small)
        results.extend((h, "") for h in small)
    if large:
        large_results = await _volume_batch_update.remote.aio(large)
        markers = {h: _VOLUME_MARKER for h, err in large_results if not err}
        if markers:
            await cas_dict.update.aio(markers)
        results.extend(large_results)
    return results


# ---------------------------------------------------------------------------
# Internal Modal Functions for the large-blob (Volume) path
# ---------------------------------------------------------------------------


def _atomic_write(path: str, data: bytes) -> None:
    ensure_parent(path)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


@app.function(image=cache_image, volumes={CAS_MOUNT: cas_volume}, max_containers=32)
def _volume_write(hash_: str, size: int, blob: bytes) -> None:
    path = cas_path(CAS_MOUNT, hash_)
    if os.path.exists(path):
        return
    _atomic_write(path, blob)
    cas_volume.commit()


def _read_with_retry(path: str) -> bytes | None:
    """Optimistic read; only reload on miss to avoid a metadata RPC per read."""
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        cas_volume.reload()
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None


@app.function(image=cache_image, volumes={CAS_MOUNT: cas_volume}, max_containers=32)
def _volume_read(hash_: str) -> bytes | None:
    return _read_with_retry(cas_path(CAS_MOUNT, hash_))


@app.function(image=cache_image, volumes={CAS_MOUNT: cas_volume}, max_containers=32)
def _volume_batch_read(hashes: list[str]) -> list[tuple[str, bytes | None]]:
    out: list[tuple[str, bytes | None]] = []
    reloaded = False
    for h in hashes:
        path = cas_path(CAS_MOUNT, h)
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


@app.function(image=cache_image, volumes={CAS_MOUNT: cas_volume}, max_containers=32)
def _volume_batch_update(blobs: list[tuple[str, int, bytes]]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    wrote = False
    for h, _sz, b in blobs:
        path = cas_path(CAS_MOUNT, h)
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
