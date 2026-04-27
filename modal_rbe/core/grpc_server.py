"""gRPC server builder.

Bundles the Capabilities / CAS / AC / ByteStream / Execution servicers
behind a `_BearerAuthInterceptor` and returns a `grpc.aio.Server` ready to
`start()` + `wait_for_termination()`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Mapping, Optional

import grpc
import grpc.aio
import modal
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.bytestream import bytestream_pb2_grpc as bs_grpc

from .cache import CacheStore
from .servicers.ac_servicer import ActionCacheServicer
from .servicers.bytestream import ByteStreamServicer
from .servicers.capabilities import CapabilitiesServicer
from .servicers.cas_servicer import ContentAddressableStorageServicer
from .servicers.execution import DEFAULT_POOL, ExecutionServicer
from .telemetry import snapshot_and_reset

log = logging.getLogger("modal_rbe")

DEFAULT_MAX_BLOB_SIZE = 4 * 1024 * 1024 * 1024  # 4 GiB
DEFAULT_MAX_BATCH_SIZE = 32 * 1024 * 1024  # 32 MiB
_GRPC_MESSAGE_LIMIT = 64 * 1024 * 1024


# ---------------------------------------------------------------------------
# Auth interceptor
# ---------------------------------------------------------------------------


async def _abort_unauthenticated(context: grpc.aio.ServicerContext) -> None:
    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid auth token")


async def _deny_uu(request, context):
    await _abort_unauthenticated(context)


async def _deny_us(request, context):
    await _abort_unauthenticated(context)
    yield  # pragma: no cover


async def _deny_su(request_iter, context):
    await _abort_unauthenticated(context)


async def _deny_ss(request_iter, context):
    await _abort_unauthenticated(context)
    yield  # pragma: no cover


def _build_deny_handler(orig: grpc.RpcMethodHandler) -> grpc.RpcMethodHandler:
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
# Server builder
# ---------------------------------------------------------------------------


def build_grpc_server(
    *,
    cache: CacheStore,
    pools: Mapping[str, "modal.Function"],
    auth_token: str,
    port: int,
    default_pool: str = DEFAULT_POOL,
    exec_enabled: bool = True,
    max_blob_size: int = DEFAULT_MAX_BLOB_SIZE,
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
) -> grpc.aio.Server:
    server = grpc.aio.server(
        interceptors=[_BearerAuthInterceptor(auth_token)],
        options=[
            ("grpc.max_send_message_length", _GRPC_MESSAGE_LIMIT),
            ("grpc.max_receive_message_length", _GRPC_MESSAGE_LIMIT),
        ],
    )
    rex_grpc.add_CapabilitiesServicer_to_server(
        CapabilitiesServicer(exec_enabled=exec_enabled, max_batch_size=max_batch_size),
        server,
    )
    rex_grpc.add_ContentAddressableStorageServicer_to_server(
        ContentAddressableStorageServicer(cache), server
    )
    rex_grpc.add_ActionCacheServicer_to_server(ActionCacheServicer(cache), server)
    bs_grpc.add_ByteStreamServicer_to_server(
        ByteStreamServicer(cache, max_blob_size=max_blob_size), server
    )
    if exec_enabled:
        rex_grpc.add_ExecutionServicer_to_server(
            ExecutionServicer(cache, pools, default_pool=default_pool), server
        )
    server.add_insecure_port(f"[::]:{port}")
    return server


async def _print_rpc_stats() -> None:
    while True:
        await asyncio.sleep(2.0)
        snap = snapshot_and_reset()
        if not snap:
            continue
        parts = [
            f"{k}: n={v['count']} avg={v['total']/v['count']*1000:.0f}ms total={v['total']:.2f}s"
            for k, v in sorted(snap.items())
        ]
        log.info("rpcs in last 2s: %s", " | ".join(parts))


async def serve_forever(
    *,
    cache: CacheStore,
    pools: Mapping[str, "modal.Function"],
    auth_token: str,
    port: int,
    default_pool: str = DEFAULT_POOL,
    exec_enabled: bool = True,
    max_blob_size: int = DEFAULT_MAX_BLOB_SIZE,
    max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
) -> None:
    """Start the gRPC server and block until termination. Wrap in
    `asyncio.run(serve_forever(...))` from a `@modal.enter()` background
    thread."""
    server = build_grpc_server(
        cache=cache,
        pools=pools,
        auth_token=auth_token,
        port=port,
        default_pool=default_pool,
        exec_enabled=exec_enabled,
        max_blob_size=max_blob_size,
        max_batch_size=max_batch_size,
    )
    await server.start()
    asyncio.create_task(_print_rpc_stats())
    log.info("RBE backend listening on :%d", port)
    await server.wait_for_termination()
