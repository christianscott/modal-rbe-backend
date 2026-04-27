"""ByteStream resource-name parsing for the Bazel Remote Execution API.

Read format:   [{instance_name}/]{compressor?}/blobs/{hash}/{size}[/...]
Write format:  [{instance_name}/]uploads/{uuid}/{compressor?}/blobs/{hash}/{size}[/...]

Where {compressor?} is either absent or one of: compressed-blobs/<algo>.
v1 only handles uncompressed (IDENTITY) blobs — compressed names are rejected.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReadResource:
    instance_name: str
    hash: str
    size: int


@dataclass(frozen=True)
class WriteResource:
    instance_name: str
    uuid: str
    hash: str
    size: int


def parse_read_resource(name: str) -> ReadResource:
    parts = name.split("/")
    try:
        idx = parts.index("blobs")
    except ValueError:
        raise ValueError(f"resource name missing 'blobs' segment: {name!r}")
    if len(parts) < idx + 3:
        raise ValueError(f"resource name truncated after 'blobs': {name!r}")
    instance_name = "/".join(parts[:idx]) if idx > 0 else ""
    # Reject compressed-blobs in v1.
    if instance_name.endswith("compressed-blobs") or "/compressed-blobs/" in instance_name:
        raise ValueError(f"compressed-blobs not supported in v1: {name!r}")
    hash_ = parts[idx + 1]
    try:
        size = int(parts[idx + 2])
    except ValueError:
        raise ValueError(f"non-integer size in resource name: {name!r}")
    # Strip trailing compressed-blobs marker from instance_name if present.
    return ReadResource(instance_name=instance_name, hash=hash_, size=size)


def parse_write_resource(name: str) -> WriteResource:
    parts = name.split("/")
    try:
        upl_idx = parts.index("uploads")
        blobs_idx = parts.index("blobs", upl_idx)
    except ValueError:
        raise ValueError(f"write resource name missing 'uploads/.../blobs/...': {name!r}")
    if upl_idx + 1 >= len(parts):
        raise ValueError(f"missing uuid in write resource: {name!r}")
    if len(parts) < blobs_idx + 3:
        raise ValueError(f"truncated write resource: {name!r}")
    instance_name = "/".join(parts[:upl_idx]) if upl_idx > 0 else ""
    uuid = parts[upl_idx + 1]
    hash_ = parts[blobs_idx + 1]
    try:
        size = int(parts[blobs_idx + 2])
    except ValueError:
        raise ValueError(f"non-integer size in write resource: {name!r}")
    if "compressed-blobs" in parts[upl_idx + 2 : blobs_idx]:
        raise ValueError(f"compressed-blobs not supported in v1: {name!r}")
    return WriteResource(instance_name=instance_name, uuid=uuid, hash=hash_, size=size)
