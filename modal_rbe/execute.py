from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import tempfile
import threading

import modal

from .app import (
    CAS_MOUNT,
    ac_dict,
    app,
    cas_dict,
    cas_volume,
    default_exec_image,
    light_exec_image,
)
from .cas import SMALL_THRESHOLD, _INLINE, _VOLUME_MARKER
from .digest import cas_path, ensure_parent

# Hard cap on action wall-clock if the Action proto doesn't carry a timeout.
DEFAULT_ACTION_TIMEOUT_S = 600
# Modal Function-level timeout (must be >= per-action timeout).
EXEC_FUNCTION_TIMEOUT = 3600


# Tracks (per-thread) whether the current action wrote any large blob to the
# volume; if so we commit at the end. A thread-local flag is required because
# `@modal.concurrent` runs concurrent actions in worker threads in the same
# process — a process-global flag would race.
_tls = threading.local()


def _mark_volume_dirty() -> None:
    _tls.volume_dirty = True


def _consume_volume_dirty() -> bool:
    flag = getattr(_tls, "volume_dirty", False)
    _tls.volume_dirty = False
    return flag


# Per-container pool of materialized CAS blobs. Survives across action
# invocations on the same container, so input materialization shrinks from
# "copy every file from CAS" to "hardlink from the pool" once a blob has been
# pulled once. Pool grows unbounded; relies on Modal's container recycling.
POOL_DIR = "/cas-pool"


def _read_blob(hash_: str) -> bytes:
    val = cas_dict.get(hash_, None)
    if val is not None:
        if val[:1] == _INLINE:
            return val[1:]
        if val[:1] == _VOLUME_MARKER:
            path = cas_path(CAS_MOUNT, hash_)
            with open(path, "rb") as f:
                return f.read()
    # Pre-marker volume blob (legacy) — fall back to a direct read.
    path = cas_path(CAS_MOUNT, hash_)
    with open(path, "rb") as f:
        return f.read()


def _ensure_in_pool(hash_: str) -> str:
    """Ensure the blob is materialized at POOL_DIR/<hash[:2]>/<hash>.

    Returns the pool path, suitable for `os.link(pool_path, target)`.
    Safe under concurrent calls (multiple threads or invocations) — the
    populate path uses a unique tempfile + atomic rename.
    """
    pool_path = os.path.join(POOL_DIR, hash_[:2], hash_)
    if os.path.exists(pool_path):
        return pool_path
    os.makedirs(os.path.dirname(pool_path), exist_ok=True)
    data = _read_blob(hash_)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(pool_path))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        # 0o755 on the inode lets per-action hardlinks pretend the file is
        # either executable or not without rewriting the inode mode (which
        # would alias across all hardlinks).
        os.chmod(tmp, 0o755)
        os.replace(tmp, pool_path)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return pool_path


def _write_blob(data: bytes) -> tuple[str, int]:
    h = hashlib.sha256(data).hexdigest()
    sz = len(data)
    if sz <= SMALL_THRESHOLD:
        if h not in cas_dict:
            cas_dict[h] = _INLINE + data
        return h, sz
    path = cas_path(CAS_MOUNT, h)
    if not os.path.exists(path):
        ensure_parent(path)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        _mark_volume_dirty()
    if h not in cas_dict:
        cas_dict[h] = _VOLUME_MARKER
    return h, sz


def _materialize_directory(directory_hash: str, dest: str) -> None:
    """Recursively materialize a Directory proto rooted at directory_hash into dest.

    Files are hardlinked from a per-container CAS blob pool. The first action
    that touches a given blob pays the file-copy cost; subsequent actions on
    the same container get a near-free `os.link`.
    """
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    blob = _read_blob(directory_hash)
    d = rex.Directory()
    d.ParseFromString(blob)
    os.makedirs(dest, exist_ok=True)
    for f in d.files:
        path = os.path.join(dest, f.name)
        pool_path = _ensure_in_pool(f.digest.hash)
        try:
            os.link(pool_path, path)
        except FileExistsError:
            os.unlink(path)
            os.link(pool_path, path)
    for sub in d.directories:
        _materialize_directory(sub.digest.hash, os.path.join(dest, sub.name))
    for sym in d.symlinks:
        link_path = os.path.join(dest, sym.name)
        os.symlink(sym.target, link_path)


def _build_directory_proto(path: str):
    """Recursively build a Directory proto for `path`. Returns (Directory, list[Directory_subtree])."""
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    d = rex.Directory()
    children: list[rex.Directory] = []
    for entry in sorted(os.listdir(path)):
        full = os.path.join(path, entry)
        st = os.lstat(full)
        if stat.S_ISLNK(st.st_mode):
            target = os.readlink(full)
            d.symlinks.add(name=entry, target=target)
        elif stat.S_ISDIR(st.st_mode):
            sub, sub_children = _build_directory_proto(full)
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
            h, sz = _write_blob(data)
            is_exec = bool(st.st_mode & 0o111)
            d.files.add(
                name=entry,
                digest=rex.Digest(hash=h, size_bytes=sz),
                is_executable=is_exec,
            )
    return d, children


def _build_tree_for_output_dir(path: str):
    """Build a Tree proto for output directory at `path` and write it to CAS."""
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    root, children = _build_directory_proto(path)
    # Persist all child directories and root in CAS too (clients may read them).
    for d in [root, *children]:
        _write_blob(d.SerializeToString())
    tree = rex.Tree(root=root, children=children)
    tree_bytes = tree.SerializeToString()
    h, sz = _write_blob(tree_bytes)
    return rex.Digest(hash=h, size_bytes=sz)


# Keep one executor container warm so the per-container hardlink pool
# (POOL_DIR) doesn't get rebuilt from cold on every build. `min_containers=1`
# pre-spawns the container after `modal deploy`; @modal.concurrent lets that
# one container handle many parallel actions on shared rootfs (so the pool
# is populated once and amortized across all subsequent actions).
_EXEC_MAX_CONTAINERS = 4
_EXEC_INPUT_CONCURRENCY = 4
# Note on disk: Modal gives every container 512 GiB of SSD-backed
# `ephemeral_disk` by default (and that's also the minimum — Modal won't
# accept a request smaller). The hardlink pool below lives on this scratch.

# ---------------------------------------------------------------------------
# Pool routing
#
# An "action pool" is a logical execution environment — one Modal Function
# with one Image and its own warm container + hardlink pool. Bazel chooses
# which pool an action goes to via an exec_property:
#
#     exec_properties = {"Pool": "light"}     # per-target
#     --remote_default_exec_properties=Pool=light    # workspace default
#
# Actions without a `Pool` property (or with an unknown one) fall through to
# the `default` pool. To add a pool: declare an Image in app.py, register it
# in EXECUTORS_BY_POOL below.
# ---------------------------------------------------------------------------

POOL_PROPERTY_KEY = "Pool"
DEFAULT_POOL = "default"


def _execute_impl(action_hash: str, action_size: int) -> bytes:
    """Action-execution body, shared by every per-pool wrapper.

    Each wrapper's only job is to bind a specific Modal Image and image-level
    Function options (min_containers, etc.); all the materialize / subprocess /
    collect / cache logic lives here.
    """
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
    from google.rpc import code_pb2, status_pb2

    cas_volume.reload()
    _consume_volume_dirty()  # reset the per-thread flag

    action = rex.Action()
    action.ParseFromString(_read_blob(action_hash))

    command = rex.Command()
    command.ParseFromString(_read_blob(action.command_digest.hash))

    timeout_s = (
        action.timeout.seconds + action.timeout.nanos / 1e9
        if action.HasField("timeout") and action.timeout.seconds
        else DEFAULT_ACTION_TIMEOUT_S
    )

    with tempfile.TemporaryDirectory(prefix="rbe-act-") as workspace:
        # Materialize input root.
        _materialize_directory(action.input_root_digest.hash, workspace)

        # Pre-create directories for declared outputs.
        out_paths = list(command.output_paths) or list(command.output_files) + list(
            command.output_directories
        )
        for op in out_paths:
            target_dir = os.path.dirname(op)
            if target_dir:
                os.makedirs(os.path.join(workspace, target_dir), exist_ok=True)

        # Run command.
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

        # Upload stdout/stderr to CAS.
        stdout_hash, stdout_size = _write_blob(stdout)
        stderr_hash, stderr_size = _write_blob(stderr)

        # Collect declared outputs.
        result = rex.ActionResult(
            exit_code=exit_code,
            stdout_digest=rex.Digest(hash=stdout_hash, size_bytes=stdout_size),
            stderr_digest=rex.Digest(hash=stderr_hash, size_bytes=stderr_size),
        )

        # In v2.1+, prefer output_paths; fall back to output_files / output_directories.
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
                tree_digest = _build_tree_for_output_dir(full)
                result.output_directories.add(path=rel, tree_digest=tree_digest)
            elif os.path.isfile(full):
                if hint == "dir":
                    continue
                with open(full, "rb") as f:
                    data = f.read()
                h, sz = _write_blob(data)
                is_exec = bool(os.stat(full).st_mode & 0o111)
                result.output_files.add(
                    path=rel,
                    digest=rex.Digest(hash=h, size_bytes=sz),
                    is_executable=is_exec,
                )


        if _consume_volume_dirty():
            cas_volume.commit()

        # Persist to AC only on success and when allowed.
        if exit_code == 0 and not action.do_not_cache and not timed_out:
            ac_dict[action_hash] = result.SerializeToString()

        response = rex.ExecuteResponse(
            result=result,
            cached_result=False,
            status=status_pb2.Status(code=code_pb2.OK),
        )
        return response.SerializeToString()


# ---------------------------------------------------------------------------
# Per-pool Modal Functions
#
# Each one is a thin wrapper around `_execute_impl`; the only thing that
# differs is the Image (and per-pool Function options like min_containers).
# Modal binds an Image at decoration time, so we can't pass the image
# through as a parameter — hence one decorated function per pool.
# ---------------------------------------------------------------------------


# Pin executors to the same region as the Flash proxy (RbeServer) so the
# in-cluster RbeServer→executor `.remote.aio()` round-trip stays in-AZ
# instead of crossing regions on every action dispatch.
_EXEC_REGION = "us-east"


@app.function(
    image=default_exec_image,
    volumes={CAS_MOUNT: cas_volume},
    timeout=EXEC_FUNCTION_TIMEOUT,
    min_containers=1,
    max_containers=_EXEC_MAX_CONTAINERS,
    region=_EXEC_REGION,
)
@modal.concurrent(max_inputs=_EXEC_INPUT_CONCURRENCY)
def execute_default(action_hash: str, action_size: int) -> bytes:
    """Default executor pool — debian_slim + build-essential + git + python3."""
    return _execute_impl(action_hash, action_size)


@app.function(
    image=light_exec_image,
    volumes={CAS_MOUNT: cas_volume},
    timeout=EXEC_FUNCTION_TIMEOUT,
    # min_containers=0 (default): scale to zero when no `Pool=light` traffic;
    # actions targeting this pool pay a cold-start tax on first request.
    max_containers=_EXEC_MAX_CONTAINERS,
    region=_EXEC_REGION,
)
@modal.concurrent(max_inputs=_EXEC_INPUT_CONCURRENCY)
def execute_light(action_hash: str, action_size: int) -> bytes:
    """Light pool — debian_slim with no apt packages. For actions whose
    command only needs a shell + python interpreter."""
    return _execute_impl(action_hash, action_size)


# Registered pools. Add new pools by declaring an Image in app.py, defining
# a wrapper Function above, and adding it here.
EXECUTORS_BY_POOL: dict[str, "modal.Function"] = {
    "default": execute_default,
    "light": execute_light,
}
