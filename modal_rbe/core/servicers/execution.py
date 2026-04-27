from __future__ import annotations

import logging
from typing import Mapping

import grpc
import modal
from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.longrunning import operations_pb2
from google.protobuf import any_pb2
from google.rpc import code_pb2, status_pb2

from ..cache import CacheStore
from .ac_servicer import output_hashes

log = logging.getLogger(__name__)

# Bazel-side exec_property name we route on.
POOL_PROPERTY_KEY = "Pool"
DEFAULT_POOL = "default"


def _pack_response(resp: rex.ExecuteResponse) -> any_pb2.Any:
    a = any_pb2.Any()
    a.Pack(resp, type_url_prefix="type.googleapis.com/")
    return a


def _pack_metadata(stage: int, action_digest: rex.Digest) -> any_pb2.Any:
    md = rex.ExecuteOperationMetadata(stage=stage, action_digest=action_digest)
    a = any_pb2.Any()
    a.Pack(md, type_url_prefix="type.googleapis.com/")
    return a


class ExecutionServicer(rex_grpc.ExecutionServicer):
    def __init__(
        self,
        cache: CacheStore,
        pools: Mapping[str, "modal.Function"],
        default_pool: str = DEFAULT_POOL,
    ) -> None:
        if default_pool not in pools:
            raise ValueError(
                f"default_pool {default_pool!r} not in pools {list(pools)}"
            )
        self._cache = cache
        self._pools = dict(pools)
        self._default_pool = default_pool

    async def _try_cached_result(
        self, action_digest: rex.Digest
    ) -> rex.ActionResult | None:
        blob = await self._cache.get_action_result(action_digest.hash)
        if blob is None:
            return None
        result = rex.ActionResult()
        result.ParseFromString(blob)
        hashes = output_hashes(result)
        if hashes:
            missing = await self._cache.find_missing(hashes)
            if missing:
                return None
        return result

    async def _select_executor(self, action_digest: rex.Digest):
        blob = await self._cache.read(action_digest.hash)
        if blob is None:
            return self._pools[self._default_pool]
        action = rex.Action()
        action.ParseFromString(blob)
        for prop in action.platform.properties:
            if prop.name == POOL_PROPERTY_KEY:
                fn = self._pools.get(prop.value)
                if fn is not None:
                    return fn
                log.warning(
                    "action %s requested unknown pool %r; routing to %s",
                    action_digest.hash, prop.value, self._default_pool,
                )
                break
        return self._pools[self._default_pool]

    async def Execute(self, request, context):  # noqa: N802
        action_digest = request.action_digest
        op_name = f"actions/{action_digest.hash}/{action_digest.size_bytes}"

        if not request.skip_cache_lookup:
            cached = await self._try_cached_result(action_digest)
            if cached is not None:
                resp = rex.ExecuteResponse(
                    result=cached,
                    cached_result=True,
                    status=status_pb2.Status(code=code_pb2.OK),
                )
                yield operations_pb2.Operation(
                    name=op_name,
                    metadata=_pack_metadata(
                        rex.ExecutionStage.COMPLETED, action_digest
                    ),
                    done=True,
                    response=_pack_response(resp),
                )
                return

        executor = await self._select_executor(action_digest)
        try:
            resp_bytes = await executor.remote.aio(
                action_digest.hash, action_digest.size_bytes
            )
        except Exception as e:  # noqa: BLE001
            log.exception("execute_action failed")
            failure = rex.ExecuteResponse(
                status=status_pb2.Status(code=code_pb2.INTERNAL, message=str(e)),
            )
            yield operations_pb2.Operation(
                name=op_name,
                metadata=_pack_metadata(rex.ExecutionStage.COMPLETED, action_digest),
                done=True,
                response=_pack_response(failure),
            )
            return

        resp = rex.ExecuteResponse()
        resp.ParseFromString(resp_bytes)
        yield operations_pb2.Operation(
            name=op_name,
            metadata=_pack_metadata(rex.ExecutionStage.COMPLETED, action_digest),
            done=True,
            response=_pack_response(resp),
        )

    async def WaitExecution(self, request, context):  # noqa: N802
        await context.abort(
            grpc.StatusCode.NOT_FOUND,
            "WaitExecution not supported; Execute returns completed operations",
        )
