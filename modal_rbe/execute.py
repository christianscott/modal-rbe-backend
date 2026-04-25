from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import tempfile

from .app import CAS_MOUNT, ac_dict, app, cas_dict, cas_volume, exec_image
from .cas import SMALL_THRESHOLD, _INLINE, _VOLUME_MARKER
from .digest import cas_path, ensure_parent

# Hard cap on action wall-clock if the Action proto doesn't carry a timeout.
DEFAULT_ACTION_TIMEOUT_S = 600
# Modal Function-level timeout (must be >= per-action timeout).
EXEC_FUNCTION_TIMEOUT = 3600


# Tracks whether the executor wrote any large blob to the volume during the
# action; if so we commit at the end, otherwise we skip the commit RPC.
_volume_dirty = {"flag": False}


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
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        _volume_dirty["flag"] = True
    if h not in cas_dict:
        cas_dict[h] = _VOLUME_MARKER
    return h, sz


def _materialize_directory(directory_hash: str, dest: str) -> None:
    """Recursively materialize a Directory proto rooted at directory_hash into dest."""
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex

    blob = _read_blob(directory_hash)
    d = rex.Directory()
    d.ParseFromString(blob)
    os.makedirs(dest, exist_ok=True)
    for f in d.files:
        path = os.path.join(dest, f.name)
        ensure_parent(path)
        with open(path, "wb") as out:
            out.write(_read_blob(f.digest.hash))
        if f.is_executable:
            os.chmod(path, 0o755)
        else:
            os.chmod(path, 0o644)
    for sub in d.directories:
        _materialize_directory(sub.digest.hash, os.path.join(dest, sub.name))
    for sym in d.symlinks:
        link_path = os.path.join(dest, sym.name)
        ensure_parent(link_path)
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


@app.function(
    image=exec_image,
    volumes={CAS_MOUNT: cas_volume},
    timeout=EXEC_FUNCTION_TIMEOUT,
    max_containers=5,
)
def execute_action(action_hash: str, action_size: int) -> bytes:
    """Run an action and return a serialized ExecuteResponse."""
    from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
    from google.rpc import code_pb2, status_pb2

    cas_volume.reload()
    _volume_dirty["flag"] = False

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

        if _volume_dirty["flag"]:
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
