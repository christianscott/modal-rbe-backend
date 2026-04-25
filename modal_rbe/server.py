from __future__ import annotations

import asyncio
import logging
import os

import modal_rbe  # noqa: F401  (side-effecting: registers proto path)

import grpc
import grpc.aio
import modal
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.bytestream import bytestream_pb2_grpc as bs_grpc

from .app import app, cache_image
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


async def _abort_unauthenticated(context: grpc.aio.ServicerContext) -> None:
    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid auth token")


async def _deny_uu(request, context):
    await _abort_unauthenticated(context)


async def _deny_us(request, context):  # unary_stream — must be a generator
    await _abort_unauthenticated(context)
    yield  # pragma: no cover  (unreachable)


async def _deny_su(request_iter, context):
    await _abort_unauthenticated(context)


async def _deny_ss(request_iter, context):
    await _abort_unauthenticated(context)
    yield  # pragma: no cover


def _build_deny_handler(orig: grpc.RpcMethodHandler) -> grpc.RpcMethodHandler:
    """Replace a handler with one that immediately aborts UNAUTHENTICATED.

    The replacement keeps the same RPC type (unary/stream) so gRPC's dispatch
    machinery doesn't trip; the abort fires before any request bytes are
    deserialized.
    """
    rd = orig.request_deserializer
    rs = orig.response_serializer
    if orig.unary_unary:
        return grpc.unary_unary_rpc_method_handler(_deny_uu, rd, rs)
    if orig.unary_stream:
        return grpc.unary_stream_rpc_method_handler(_deny_us, rd, rs)
    if orig.stream_unary:
        return grpc.stream_unary_rpc_method_handler(_deny_su, rd, rs)
    return grpc.stream_stream_rpc_method_handler(_deny_ss, rd, rs)


class _BearerAuthInterceptor(grpc.aio.ServerInterceptor):
    """Reject any RPC whose `authorization` metadata isn't `Bearer <token>`."""

    def __init__(self, token: str) -> None:
        self._expected = f"Bearer {token}"

    async def intercept_service(self, continuation, handler_call_details):
        for k, v in handler_call_details.invocation_metadata or ():
            if k.lower() == "authorization" and v == self._expected:
                return await continuation(handler_call_details)
        original = await continuation(handler_call_details)
        return _build_deny_handler(original)


def _build_server(
    port: int,
    exec_enabled: bool,
    max_blob_size: int,
    max_batch_size: int,
    auth_token: str | None = None,
) -> grpc.aio.Server:
    interceptors = []
    if auth_token:
        interceptors.append(_BearerAuthInterceptor(auth_token))
    server = grpc.aio.server(interceptors=interceptors) if interceptors else grpc.aio.server()
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
    """Run the gRPC server locally; clients use grpc://localhost:50051."""
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


# ---------------------------------------------------------------------------
# In-cluster server: runs the gRPC server inside a Modal container so that
# every Dict / Volume / Function call is in-cluster (sub-ms) instead of
# paying ~40 ms per Modal API hop from a developer laptop. Clients reach the
# server via a `modal.forward` HTTPS+H2 tunnel.
# ---------------------------------------------------------------------------

# Long timeout so a single invocation can serve a long Bazel session.
_REMOTE_SERVE_TIMEOUT = 24 * 60 * 60

# Modal Secret that holds the bearer token. Create/refresh with the
# `init_auth_secret` local_entrypoint below.
_AUTH_SECRET_NAME = "rbe-auth-token"
_AUTH_SECRET_KEY = "MODAL_RBE_AUTH_TOKEN"
_auth_secret = modal.Secret.from_name(
    _AUTH_SECRET_NAME, required_keys=[_AUTH_SECRET_KEY]
)


async def _serve_with_tunnel(
    port: int,
    exec_enabled: bool,
    max_blob_size: int,
    max_batch_size: int,
    auth_token: str,
) -> None:
    server = _build_server(
        port=port,
        exec_enabled=exec_enabled,
        max_blob_size=max_blob_size,
        max_batch_size=max_batch_size,
        auth_token=auth_token,
    )
    await server.start()
    asyncio.create_task(_print_rpc_stats())
    async with modal.forward(port, h2_enabled=True) as tunnel:
        grpcs_url = tunnel.url.replace("https://", "grpcs://", 1)
        log.info("RBE backend listening at %s (in-cluster)", tunnel.url)
        log.info("")
        log.info("Configure Bazel with:")
        log.info("    --remote_cache=%s", grpcs_url)
        if exec_enabled:
            log.info("    --remote_executor=%s", grpcs_url)
        log.info("    --remote_header=authorization=Bearer %s", auth_token)
        log.info("")
        await server.wait_for_termination()


@app.function(
    image=cache_image,
    secrets=[_auth_secret],
    min_containers=1,
    max_containers=1,
    timeout=_REMOTE_SERVE_TIMEOUT,
)
def serve_remote(
    port: int = 50051,
    exec_enabled: bool = True,
    max_blob_size: int = DEFAULT_MAX_BLOB_SIZE,
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
) -> None:
    """Run the gRPC server inside a Modal container, exposed via HTTPS+H2.

    Invoke with ``modal run -m modal_rbe.server::serve_remote`` — the function
    will print a `grpcs://...modal.host` URL plus the Bearer token for Bazel
    to use. The token is read from the `rbe-auth-token` Modal Secret; create
    it once with ``modal run -m modal_rbe.server::init_auth_secret``.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    token = os.environ[_AUTH_SECRET_KEY]
    asyncio.run(
        _serve_with_tunnel(
            port=port,
            exec_enabled=exec_enabled,
            max_blob_size=max_blob_size,
            max_batch_size=max_batch_size,
            auth_token=token,
        )
    )


