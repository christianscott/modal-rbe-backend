from __future__ import annotations

import asyncio
import logging
import os
import threading

import modal_rbe  # noqa: F401  (side-effecting: registers proto path)

import grpc
import grpc.aio
import modal
import modal.experimental
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

# Cap on a single ByteStream blob upload (4 GiB).
MAX_BLOB_SIZE = 4 * 1024 * 1024 * 1024
# Cap advertised in GetCapabilities for batched RPCs. We override gRPC's
# 4 MiB default so Bazel can pack more blobs per BatchReadBlobs/BatchUpdate
# RPC (each message has fixed RTT cost; bigger batches = fewer round-trips
# during the link action's input fetch). The server's gRPC channel is
# configured with matching send/receive limits below.
MAX_BATCH_SIZE = 32 * 1024 * 1024
# Headroom over MAX_BATCH_SIZE so framing/proto overhead doesn't trip the limit.
_GRPC_MESSAGE_LIMIT = 64 * 1024 * 1024
PORT = 50051
# Long timeout — the container hosts a long-running gRPC server.
SERVE_TIMEOUT = 24 * 60 * 60

# Bearer token storage. Bootstrap with `python -m modal_rbe.setup_secret`.
_AUTH_SECRET_NAME = "rbe-auth-token"
_AUTH_SECRET_KEY = "MODAL_RBE_AUTH_TOKEN"
_auth_secret = modal.Secret.from_name(
    _AUTH_SECRET_NAME, required_keys=[_AUTH_SECRET_KEY]
)

# `@modal.experimental.http_server` gives the deployed class a stable URL
# bound to the app + class name (printed by `modal deploy`), so no separate
# URL-discovery step is needed. Pin the proxy + executor region so request
# RTT is one well-known hop.
_PROXY_REGION = "us-east"


# ---------------------------------------------------------------------------
# Auth interceptor
# ---------------------------------------------------------------------------


async def _abort_unauthenticated(context: grpc.aio.ServicerContext) -> None:
    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid auth token")


async def _deny_uu(request, context):
    await _abort_unauthenticated(context)


async def _deny_us(request, context):  # unary_stream — must be a generator
    await _abort_unauthenticated(context)
    yield  # pragma: no cover


async def _deny_su(request_iter, context):
    await _abort_unauthenticated(context)


async def _deny_ss(request_iter, context):
    await _abort_unauthenticated(context)
    yield  # pragma: no cover


def _build_deny_handler(orig: grpc.RpcMethodHandler) -> grpc.RpcMethodHandler:
    """Replacement handler that aborts UNAUTHENTICATED while preserving the
    RPC's stream type so gRPC's dispatch machinery doesn't trip."""
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
    def __init__(self, token: str) -> None:
        self._expected = f"Bearer {token}"

    async def intercept_service(self, continuation, handler_call_details):
        for k, v in handler_call_details.invocation_metadata or ():
            if k.lower() == "authorization" and v == self._expected:
                return await continuation(handler_call_details)
        original = await continuation(handler_call_details)
        return _build_deny_handler(original)


# ---------------------------------------------------------------------------
# Server bootstrap
# ---------------------------------------------------------------------------


def _build_server(auth_token: str) -> grpc.aio.Server:
    server = grpc.aio.server(
        interceptors=[_BearerAuthInterceptor(auth_token)],
        options=[
            ("grpc.max_send_message_length", _GRPC_MESSAGE_LIMIT),
            ("grpc.max_receive_message_length", _GRPC_MESSAGE_LIMIT),
        ],
    )
    rex_grpc.add_CapabilitiesServicer_to_server(
        CapabilitiesServicer(exec_enabled=True, max_batch_size=MAX_BATCH_SIZE),
        server,
    )
    rex_grpc.add_ContentAddressableStorageServicer_to_server(
        ContentAddressableStorageServicer(), server
    )
    rex_grpc.add_ActionCacheServicer_to_server(ActionCacheServicer(), server)
    bs_grpc.add_ByteStreamServicer_to_server(
        ByteStreamServicer(max_blob_size=MAX_BLOB_SIZE), server
    )
    rex_grpc.add_ExecutionServicer_to_server(ExecutionServicer(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    return server


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


async def _serve(auth_token: str) -> None:
    server = _build_server(auth_token)
    await server.start()
    asyncio.create_task(_print_rpc_stats())
    log.info("RBE backend listening on :%d (Flash HTTP/2 proxy)", PORT)
    await server.wait_for_termination()


# ---------------------------------------------------------------------------
# Deployable class
#
# `@modal.experimental.http_server(h2_enabled=True)` puts the gRPC server
# behind Modal's Flash HTTP/2 edge proxy with a stable URL bound to the
# deployed app + class name (no per-container URL discovery, lower RTT
# than `modal.forward`-per-container). Pin the class region to the same
# region as `proxy_regions` to keep the proxy↔container hop in-AZ.
# ---------------------------------------------------------------------------


@app.cls(
    image=cache_image,
    secrets=[_auth_secret],
    min_containers=1,
    max_containers=1,
    timeout=SERVE_TIMEOUT,
    region=_PROXY_REGION,
)
@modal.experimental.http_server(
    port=PORT,
    proxy_regions=[_PROXY_REGION],
    h2_enabled=True,
)
class RbeServer:
    @modal.enter()
    def start(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        token = os.environ[_AUTH_SECRET_KEY]

        def _run() -> None:
            try:
                asyncio.run(_serve(token))
            except Exception:  # noqa: BLE001
                log.exception("gRPC server crashed; container will exit")
                os._exit(1)

        # daemon=False keeps the process alive for the container's lifetime.
        threading.Thread(target=_run, daemon=False, name="rbe-grpc").start()
