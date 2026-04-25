from __future__ import annotations

import logging

import grpc
from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.longrunning import operations_pb2
from google.protobuf import any_pb2
from google.rpc import code_pb2, status_pb2

from .. import cas as cas_store
from .. import execute as exec_mod
from ..app import ac_dict
from .ac_servicer import _output_hashes

log = logging.getLogger(__name__)


def _pack_response(resp: rex.ExecuteResponse) -> any_pb2.Any:
    a = any_pb2.Any()
    a.Pack(resp, type_url_prefix="type.googleapis.com/")
    return a


def _pack_metadata(stage: int, action_digest: rex.Digest) -> any_pb2.Any:
    md = rex.ExecuteOperationMetadata(stage=stage, action_digest=action_digest)
    a = any_pb2.Any()
    a.Pack(md, type_url_prefix="type.googleapis.com/")
    return a


async def _try_cached_result(action_digest: rex.Digest) -> rex.ActionResult | None:
    blob = await ac_dict.get.aio(action_digest.hash, None)
    if blob is None:
        return None
    result = rex.ActionResult()
    result.ParseFromString(blob)
    hashes = _output_hashes(result)
    if hashes:
        missing = await cas_store.find_missing(hashes)
        if missing:
            return None
    return result


class ExecutionServicer(rex_grpc.ExecutionServicer):
    async def Execute(self, request, context):  # noqa: N802
        action_digest = request.action_digest
        op_name = f"actions/{action_digest.hash}/{action_digest.size_bytes}"

        if not request.skip_cache_lookup:
            cached = await _try_cached_result(action_digest)
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

        try:
            resp_bytes = await exec_mod.execute_action.remote.aio(
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
