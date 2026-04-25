from __future__ import annotations

import logging

import grpc
from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc

from .. import cas as cas_store
from ..app import ac_dict
from ..telemetry import timed

log = logging.getLogger(__name__)


def _output_hashes(result: rex.ActionResult) -> list[str]:
    hashes: list[str] = []
    for f in result.output_files:
        hashes.append(f.digest.hash)
    for d in result.output_directories:
        hashes.append(d.tree_digest.hash)
    if result.HasField("stdout_digest") and result.stdout_digest.hash:
        hashes.append(result.stdout_digest.hash)
    if result.HasField("stderr_digest") and result.stderr_digest.hash:
        hashes.append(result.stderr_digest.hash)
    return hashes


class ActionCacheServicer(rex_grpc.ActionCacheServicer):
    async def GetActionResult(self, request, context):  # noqa: N802
      with timed("GetActionResult"):
        h = request.action_digest.hash
        blob = await ac_dict.get.aio(h, None)
        if blob is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"AC miss for {h}")
            return
        result = rex.ActionResult()
        result.ParseFromString(blob)
        hashes = _output_hashes(result)
        if hashes:
            missing = await cas_store.find_missing(hashes)
            if missing:
                log.info(
                    "AC freshness miss for %s: %d/%d outputs missing (e.g. %s)",
                    h, len(missing), len(hashes), missing[0],
                )
                await context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"AC entry for {h} references missing CAS blobs",
                )
                return
        return result

    async def UpdateActionResult(self, request, context):  # noqa: N802
      with timed("UpdateActionResult"):
        h = request.action_digest.hash
        await ac_dict.put.aio(h, request.action_result.SerializeToString())
        return request.action_result
