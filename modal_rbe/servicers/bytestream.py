from __future__ import annotations

import logging

import grpc
from google.bytestream import bytestream_pb2 as bs
from google.bytestream import bytestream_pb2_grpc as bs_grpc

from .. import cas as cas_store
from ..resource_name import parse_read_resource, parse_write_resource
from ..telemetry import timed

log = logging.getLogger(__name__)

# Chunk size when streaming reads back to Bazel.
_READ_CHUNK = 64 * 1024


class ByteStreamServicer(bs_grpc.ByteStreamServicer):
    def __init__(self, max_blob_size: int) -> None:
        self._max_blob_size = max_blob_size

    async def Read(self, request, context):  # noqa: N802
        try:
            res = parse_read_resource(request.resource_name)
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return
        with timed("ByteStream.Read"):
            blob = await cas_store.read(res.hash)
        if blob is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"blob {res.hash} not in CAS"
            )
            return
        if len(blob) != res.size:
            await context.abort(
                grpc.StatusCode.DATA_LOSS,
                f"size mismatch for {res.hash}: expected {res.size}, got {len(blob)}",
            )
            return
        offset = request.read_offset
        limit = request.read_limit or (len(blob) - offset)
        end = min(len(blob), offset + limit)
        i = offset
        while i < end:
            j = min(end, i + _READ_CHUNK)
            yield bs.ReadResponse(data=blob[i:j])
            i = j

    async def Write(self, request_iterator, context):  # noqa: N802
      with timed("ByteStream.Write"):
        resource_name: str | None = None
        buf = bytearray()
        finished = False
        committed_offset = 0
        async for req in request_iterator:
            if resource_name is None:
                if not req.resource_name:
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "first Write request must set resource_name",
                    )
                    return
                resource_name = req.resource_name
            elif req.resource_name and req.resource_name != resource_name:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "resource_name changed mid-stream",
                )
                return
            if req.write_offset != len(buf):
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"non-sequential write_offset {req.write_offset} (have {len(buf)})",
                )
                return
            buf.extend(req.data)
            if len(buf) > self._max_blob_size:
                await context.abort(
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    f"blob exceeds max size {self._max_blob_size}",
                )
                return
            committed_offset = len(buf)
            if req.finish_write:
                finished = True
                break
        if resource_name is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty stream")
            return
        if not finished:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "stream ended without finish_write=true",
            )
            return
        try:
            res = parse_write_resource(resource_name)
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return
        if len(buf) != res.size:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"declared size {res.size} != received {len(buf)}",
            )
            return
        try:
            await cas_store.write(res.hash, res.size, bytes(buf))
        except Exception as e:  # noqa: BLE001
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return
        return bs.WriteResponse(committed_size=committed_offset)

    async def QueryWriteStatus(self, request, context):  # noqa: N802
        await context.abort(
            grpc.StatusCode.NOT_FOUND,
            "QueryWriteStatus is not supported; restart upload",
        )
