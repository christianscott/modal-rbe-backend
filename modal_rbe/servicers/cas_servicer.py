from __future__ import annotations

import logging

import grpc
from build.bazel.remote.execution.v2 import remote_execution_pb2 as rex
from build.bazel.remote.execution.v2 import remote_execution_pb2_grpc as rex_grpc
from google.rpc import code_pb2, status_pb2

from .. import cas as cas_store
from ..telemetry import timed

log = logging.getLogger(__name__)


def _ok() -> status_pb2.Status:
    return status_pb2.Status(code=code_pb2.OK)


def _err(code: int, msg: str) -> status_pb2.Status:
    return status_pb2.Status(code=code, message=msg)


class ContentAddressableStorageServicer(rex_grpc.ContentAddressableStorageServicer):
    async def FindMissingBlobs(self, request, context):  # noqa: N802
        with timed(f"FindMissingBlobs[n={len(request.blob_digests)}]"):
            hashes = [d.hash for d in request.blob_digests]
            size_by_hash = {d.hash: d.size_bytes for d in request.blob_digests}
            missing_hashes = await cas_store.find_missing(hashes)
        return rex.FindMissingBlobsResponse(
            missing_blob_digests=[
                rex.Digest(hash=h, size_bytes=size_by_hash[h]) for h in missing_hashes
            ],
        )

    async def BatchReadBlobs(self, request, context):  # noqa: N802
        with timed(f"BatchReadBlobs[n={len(request.digests)}]"):
            hashes = [d.hash for d in request.digests]
            size_by_hash = {d.hash: d.size_bytes for d in request.digests}
            results = await cas_store.batch_read(hashes)
        responses = []
        for h, blob in results:
            d = rex.Digest(hash=h, size_bytes=size_by_hash[h])
            if blob is None:
                responses.append(
                    rex.BatchReadBlobsResponse.Response(
                        digest=d,
                        status=_err(code_pb2.NOT_FOUND, f"blob {h} not found"),
                    )
                )
            else:
                responses.append(
                    rex.BatchReadBlobsResponse.Response(
                        digest=d, data=blob, status=_ok()
                    )
                )
        return rex.BatchReadBlobsResponse(responses=responses)

    async def BatchUpdateBlobs(self, request, context):  # noqa: N802
      with timed(f"BatchUpdateBlobs[n={len(request.requests)}]"):
        blobs = [
            (r.digest.hash, r.digest.size_bytes, r.data) for r in request.requests
        ]
        results = await cas_store.batch_update(blobs)
        size_by_hash = {r.digest.hash: r.digest.size_bytes for r in request.requests}
        responses = []
        for h, err in results:
            d = rex.Digest(hash=h, size_bytes=size_by_hash[h])
            if err:
                responses.append(
                    rex.BatchUpdateBlobsResponse.Response(
                        digest=d,
                        status=_err(code_pb2.INVALID_ARGUMENT, err),
                    )
                )
            else:
                responses.append(
                    rex.BatchUpdateBlobsResponse.Response(digest=d, status=_ok())
                )
        return rex.BatchUpdateBlobsResponse(responses=responses)

    async def GetTree(self, request, context):  # noqa: N802
        page_size = request.page_size or 1000
        stack = [request.root_digest.hash]
        seen: set[str] = set()
        directories: list[rex.Directory] = []
        while stack:
            h = stack.pop()
            if h in seen:
                continue
            seen.add(h)
            blob = await cas_store.read(h)
            if blob is None:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"directory {h} not in CAS"
                )
                return
            d = rex.Directory()
            d.ParseFromString(blob)
            directories.append(d)
            for child in d.directories:
                stack.append(child.digest.hash)
            if len(directories) >= page_size:
                yield rex.GetTreeResponse(directories=directories)
                directories = []
        if directories:
            yield rex.GetTreeResponse(directories=directories)
