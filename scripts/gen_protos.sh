#!/usr/bin/env bash
# Regenerate Python bindings for the vendored .proto files.
#
# Vendored sources are pinned at:
#   bazelbuild/remote-apis @ becdd8f9ff811df88a22d3eadd6341753d51d167
#   googleapis/googleapis  @ 114cd0bbdca6dfc245873fbf1d00ca2f4532db12
#
# Well-known types (google/protobuf/*.proto) are pulled in automatically by
# grpcio-tools so we do not need to vendor them.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT=modal_rbe/_proto
rm -rf "$OUT"
mkdir -p "$OUT"

PROTOS=(
    protos/build/bazel/semver/semver.proto
    protos/build/bazel/remote/execution/v2/remote_execution.proto
    protos/google/api/annotations.proto
    protos/google/api/http.proto
    protos/google/api/client.proto
    protos/google/api/field_behavior.proto
    protos/google/api/launch_stage.proto
    protos/google/longrunning/operations.proto
    protos/google/rpc/status.proto
    protos/google/rpc/code.proto
    protos/google/bytestream/bytestream.proto
)

python -m grpc_tools.protoc \
    -Iprotos \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    --pyi_out="$OUT" \
    "${PROTOS[@]}"

# Do NOT add __init__.py files. The generated tree shares the `google.*`
# top-level with the protobuf runtime, so every dir must remain a PEP 420
# namespace package. modal_rbe/_proto is itself added to sys.path by
# modal_rbe/_proto_path.py at import time.

echo "Generated protos in $OUT"
