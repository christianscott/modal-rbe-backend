"""Action-execution body, shared by every per-pool wrapper.

A user's `app.py` typically defines one `@app.function`-decorated wrapper
per pool image; the wrapper's job is just to call `execute_action_impl(...)`
with the configured `CacheStore`.

The body materializes the input tree (using a per-container hardlink pool
under POOL_DIR), runs the command via subprocess, collects outputs, and
writes the resulting `ActionResult` to the AC.
"""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import tempfile
import threading

from .cache import CacheStore, _INLINE, _VOLUME_MARKER
from .digest import cas_path, ensure_parent

# Hard cap on action wall-clock if the Action proto doesn't carry a timeout.
DEFAULT_ACTION_TIMEOUT_S = 600

# Per-container pool of materialized CAS blobs. Persists across action
# invocations on the same container, so input materialization shrinks from
# "copy every file from CAS" to "hardlink from the pool" once a blob has
# been pulled once.
POOL_DIR = "/cas-pool"

# Thread-local: did this action write any large blob to the volume?
# `@modal.concurrent` runs concurrent actions in worker threads in the same
# process, so a process-global flag would race.
_tls = threading.local()


def _mark_volume_dirty() -> None:
    _tls.volume_dirty = True


def _consume_volume_dirty() -> bool:
    flag = getattr(_tls, "volume_dirty", False)
    _tls.volume_dirty = False
    return flag


# --------------------------------------------------------------------------
# Direct (in-container) blob I/O.
#
# These run inside an executor container that has the cas_volume mounted.
# They read from the dict (in-memory hot path) and fall through to the
# mounted volume for large blobs.
# --------------------------------------------------------------------------


def _read_blob_via_cache(cache: CacheStore, hash_: str) -> bytes:
    val = cache.cas_dict.get(hash_, None)
    if val is not None:
        if val[:1] == _INLINE:
            return val[1:]
        if val[:1] == _VOLUME_MARKER:
            with open(cas_path(cache.cas_mount, hash_), "rb") as f:
                return f.read()
    # Pre-marker volume blob (legacy) — fall back to a direct read.
    with open(cas_path(cache.cas_mount, hash_), "rb") as f:
        return f.read()


def _write_blob_via_cache(cache: CacheStore, data: bytes) -> tuple[str, int]:
    h = hashlib.sha256(data).hexdigest()
    sz = len(data)
    if sz <= cache.small_threshold:
        if h not in cache.cas_dict:
            cache.cas_dict[h] = _INLINE + data
        return h, sz
    path = cas_path(cache.cas_mount, h)
    if not os.path.exists(path):
        ensure_parent(path)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        _mark_volume_dirty()
    if h not in cache.cas_dict:
        cache.cas_dict[h] = _VOLUME_MARKER
    return h, sz


def _ensure_in_pool(cache: CacheStore, hash_: str) -> str:
    """Materialize the blob into the per-container pool; return its path.

    Safe under concurrent calls: writes to a unique tempfile then atomically
    renames into place.
    """
    pool_path = os.path.join(POOL_DIR, hash_[:2], hash_)
    if os.path.exists(pool_path):
        return pool_path
    os.makedirs(os.path.dirname(pool_path), exist_ok=True)
    data = _read_blob_via_cache(cache, hash_)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(pool_path))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        # 0o755 on the inode lets per-action hardlinks pretend the file is
        # either executable or not without rewriting the inode mode.
        os.chmod(tmp, 0o755)
        os.replace(tmp, pool_path)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return pool_path


def _materialize_directory(cache: CacheStore, directory_hash: str, dest: str) -> None:
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    blob = _read_blob_via_cache(cache, directory_hash)
    d = rex.Directory()
    d.ParseFromString(blob)
    os.makedirs(dest, exist_ok=True)
    for f in d.files:
        path = os.path.join(dest, f.name)
        pool_path = _ensure_in_pool(cache, f.digest.hash)
        try:
            os.link(pool_path, path)
        except FileExistsError:
            os.unlink(path)
            os.link(pool_path, path)
    for sub in d.directories:
        _materialize_directory(cache, sub.digest.hash, os.path.join(dest, sub.name))
    for sym in d.symlinks:
        link_path = os.path.join(dest, sym.name)
        os.symlink(sym.target, link_path)


def _build_directory_proto(cache: CacheStore, path: str):
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    d = rex.Directory()
    children: list[rex.Directory] = []
    for entry in sorted(os.listdir(path)):
        full = os.path.join(path, entry)
        st = os.lstat(full)
        if stat.S_ISLNK(st.st_mode):
            d.symlinks.add(name=entry, target=os.readlink(full))
        elif stat.S_ISDIR(st.st_mode):
            sub, sub_children = _build_directory_proto(cache, full)
            sub_bytes = sub.SerializeToString()
            sub_h = hashlib.sha256(sub_bytes).hexdigest()
            d.directories.add(
                name=entry,
                digest=rex.Digest(hash=sub_h, size_bytes=len(sub_bytes)),
            )
            children.append(sub)
            children.extend(sub_children)
        elif stat.S_ISREG(st.st_mode):
            with open(full, "rb") as f:
                data = f.read()
            h, sz = _write_blob_via_cache(cache, data)
            is_exec = bool(st.st_mode & 0o111)
            d.files.add(
                name=entry,
                digest=rex.Digest(hash=h, size_bytes=sz),
                is_executable=is_exec,
            )
    return d, children


def _build_tree_for_output_dir(cache: CacheStore, path: str):
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    root, children = _build_directory_proto(cache, path)
    for d in [root, *children]:
        _write_blob_via_cache(cache, d.SerializeToString())
    tree_bytes = rex.Tree(root=root, children=children).SerializeToString()
    h, sz = _write_blob_via_cache(cache, tree_bytes)
    return rex.Digest(hash=h, size_bytes=sz)


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


def execute_action_impl(
    action_hash: str,
    action_size: int,
    *,
    cache: CacheStore,
) -> bytes:
    """Run a Bazel action and return the serialized `ExecuteResponse`.

    Wrap this in a per-pool `@app.function`-decorated function in your
    `app.py`; that's the unit Modal sees as the executor. This body is
    image-agnostic — the wrapping decorator decides the toolchain.
    """
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
    from google.rpc import code_pb2, status_pb2

    cache.cas_volume.reload()
    _consume_volume_dirty()

    action = rex.Action()
    action.ParseFromString(_read_blob_via_cache(cache, action_hash))

    command = rex.Command()
    command.ParseFromString(_read_blob_via_cache(cache, action.command_digest.hash))

    timeout_s = (
        action.timeout.seconds + action.timeout.nanos / 1e9
        if action.HasField("timeout") and action.timeout.seconds
        else DEFAULT_ACTION_TIMEOUT_S
    )

    with tempfile.TemporaryDirectory(prefix="rbe-act-") as workspace:
        _materialize_directory(cache, action.input_root_digest.hash, workspace)

        out_paths = list(command.output_paths) or list(command.output_files) + list(
            command.output_directories
        )
        for op in out_paths:
            target_dir = os.path.dirname(op)
            if target_dir:
                os.makedirs(os.path.join(workspace, target_dir), exist_ok=True)

        env = {ev.name: ev.value for ev in command.environment_variables}
        cwd = (
            os.path.join(workspace, command.working_directory)
            if command.working_directory
            else workspace
        )

        try:
            proc = subprocess.run(
                list(command.arguments),
                cwd=cwd,
                env=env,
                capture_output=True,
                timeout=timeout_s,
            )
            exit_code = proc.returncode
            stdout = proc.stdout
            stderr = proc.stderr
            timed_out = False
        except subprocess.TimeoutExpired as e:
            exit_code = -1
            stdout = e.stdout or b""
            stderr = (e.stderr or b"") + b"\n[modal-rbe] action timed out"
            timed_out = True
        except FileNotFoundError as e:
            exit_code = 127
            stdout = b""
            stderr = f"[modal-rbe] command not found: {e}\n".encode()
            timed_out = False

        stdout_hash, stdout_size = _write_blob_via_cache(cache, stdout)
        stderr_hash, stderr_size = _write_blob_via_cache(cache, stderr)

        result = rex.ActionResult(
            exit_code=exit_code,
            stdout_digest=rex.Digest(hash=stdout_hash, size_bytes=stdout_size),
            stderr_digest=rex.Digest(hash=stderr_hash, size_bytes=stderr_size),
        )

        if command.output_paths:
            paths_to_collect = [(p, None) for p in command.output_paths]
        else:
            paths_to_collect = [(p, "file") for p in command.output_files] + [
                (p, "dir") for p in command.output_directories
            ]

        for rel, hint in paths_to_collect:
            full = os.path.join(workspace, rel)
            if not os.path.exists(full) and not os.path.islink(full):
                continue
            if os.path.islink(full):
                target = os.readlink(full)
                result.output_symlinks.add(path=rel, target=target)
                continue
            if os.path.isdir(full):
                if hint == "file":
                    continue
                tree_digest = _build_tree_for_output_dir(cache, full)
                result.output_directories.add(path=rel, tree_digest=tree_digest)
            elif os.path.isfile(full):
                if hint == "dir":
                    continue
                with open(full, "rb") as f:
                    data = f.read()
                h, sz = _write_blob_via_cache(cache, data)
                is_exec = bool(os.stat(full).st_mode & 0o111)
                result.output_files.add(
                    path=rel,
                    digest=rex.Digest(hash=h, size_bytes=sz),
                    is_executable=is_exec,
                )

        if _consume_volume_dirty():
            cache.cas_volume.commit()

        if exit_code == 0 and not action.do_not_cache and not timed_out:
            cache.ac_dict[action_hash] = result.SerializeToString()

        response = rex.ExecuteResponse(
            result=result,
            cached_result=False,
            status=status_pb2.Status(code=code_pb2.OK),
        )
        return response.SerializeToString()
