from __future__ import annotations

from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from build.bazel.semver import semver_pb2

_API_VERSION = semver_pb2.SemVer(major=2, minor=3)


class CapabilitiesServicer(rex_grpc.CapabilitiesServicer):
    def __init__(self, exec_enabled: bool, max_batch_size: int) -> None:
        self._exec_enabled = exec_enabled
        self._max_batch_size = max_batch_size

    async def GetCapabilities(self, request, context):  # noqa: N802
        cache_caps = rex.CacheCapabilities(
            digest_functions=[rex.DigestFunction.SHA256],
            action_cache_update_capabilities=rex.ActionCacheUpdateCapabilities(
                update_enabled=True,
            ),
            max_batch_total_size_bytes=self._max_batch_size,
            symlink_absolute_path_strategy=rex.SymlinkAbsolutePathStrategy.ALLOWED,
            supported_compressors=[rex.Compressor.IDENTITY],
            supported_batch_update_compressors=[rex.Compressor.IDENTITY],
        )
        exec_caps = rex.ExecutionCapabilities(
            digest_function=rex.DigestFunction.SHA256,
            exec_enabled=self._exec_enabled,
            digest_functions=[rex.DigestFunction.SHA256],
        )
        return rex.ServerCapabilities(
            cache_capabilities=cache_caps,
            execution_capabilities=exec_caps,
            low_api_version=_API_VERSION,
            high_api_version=_API_VERSION,
        )
