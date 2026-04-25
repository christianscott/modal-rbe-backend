from __future__ import annotations

import asyncio
import logging

import modal_rbe  # noqa: F401  (side-effecting: registers proto path)

import grpc
import grpc.aio
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.bytestream import bytestream_pb2_grpc as bs_grpc

from .app import app
from . import cas as _cas  # noqa: F401  (registers _volume_* Functions)
from . import execute as _execute  # noqa: F401  (registers execute_action)
from .servicers.ac_servicer import ActionCacheServicer
from .servicers.bytestream import ByteStreamServicer
from .servicers.capabilities import CapabilitiesServicer
from .servicers.cas_servicer import ContentAddressableStorageServicer
from .servicers.execution import ExecutionServicer

log = logging.getLogger("modal_rbe")

# Default cap on a single blob in ByteStream uploads (4 GiB).
DEFAULT_MAX_BLOB_SIZE = 4 * 1024 * 1024 * 1024

# Cap on a single BatchUpdate/BatchRead request total bytes (4 MiB — gRPC
# default message size). Bazel respects this and chunks larger blobs through
# ByteStream.
DEFAULT_MAX_BATCH_SIZE = 4 * 1024 * 1024


def _build_server(
    port: int,
    exec_enabled: bool,
    max_blob_size: int,
    max_batch_size: int,
) -> grpc.aio.Server:
    server = grpc.aio.server()
    rex_grpc.add_CapabilitiesServicer_to_server(
        CapabilitiesServicer(
            exec_enabled=exec_enabled, max_batch_size=max_batch_size
        ),
        server,
    )
    rex_grpc.add_ContentAddressableStorageServicer_to_server(
        ContentAddressableStorageServicer(), server
    )
    rex_grpc.add_ActionCacheServicer_to_server(ActionCacheServicer(), server)
    bs_grpc.add_ByteStreamServicer_to_server(
        ByteStreamServicer(max_blob_size=max_blob_size), server
    )

    if exec_enabled:
        rex_grpc.add_ExecutionServicer_to_server(ExecutionServicer(), server)

    server.add_insecure_port(f"[::]:{port}")
    return server


async def _serve(
    port: int,
    exec_enabled: bool,
    max_blob_size: int,
    max_batch_size: int,
) -> None:
    server = _build_server(
        port=port,
        exec_enabled=exec_enabled,
        max_blob_size=max_blob_size,
        max_batch_size=max_batch_size,
    )
    await server.start()
    log.info(
        "RBE backend listening on grpc://localhost:%d (exec_enabled=%s)",
        port,
        exec_enabled,
    )
    asyncio.create_task(_print_rpc_stats())
    await server.wait_for_termination()


async def _print_rpc_stats() -> None:
    from . import telemetry

    while True:
        await asyncio.sleep(2.0)
        snap = telemetry.snapshot_and_reset()
        if not snap:
            continue
        parts = [
            f"{k}: n={v['count']} avg={v['total']/v['count']*1000:.0f}ms total={v['total']:.2f}s"
            for k, v in sorted(snap.items())
        ]
        log.info("rpcs in last 2s: %s", " | ".join(parts))


@app.local_entrypoint()
def serve(
    port: int = 50051,
    exec_enabled: bool = True,
    max_blob_size: int = DEFAULT_MAX_BLOB_SIZE,
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(
            _serve(
                port=port,
                exec_enabled=exec_enabled,
                max_blob_size=max_blob_size,
                max_batch_size=max_batch_size,
            )
        )
    except KeyboardInterrupt:
        pass
