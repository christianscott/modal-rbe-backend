# modal-rbe-backend

A Bazel Remote Execution / Remote Cache backend that runs as a localhost gRPC
server and proxies all traffic into [Modal](https://modal.com). Actions execute
inside a Modal Function; cache state is split between a `modal.Dict` (hot path)
and a `modal.Volume` (large blobs).

```
 Bazel ──gRPC──▶ local server (modal local_entrypoint)
                     │
                     ├── AC ──────▶ modal.Dict   "rbe-ac"        (ActionResult protos)
                     ├── CAS small ▶ modal.Dict   "rbe-cas-small" (≤4 MiB blobs)
                     ├── CAS large ▶ Modal Function ▶ modal.Volume "rbe-cas"
                     └── execute ──▶ Modal Function (fixed image, mounts cas_volume)
```

The split keeps the hot paths (FindMissingBlobs, BatchReadBlobs, AC lookups)
on a strongly-consistent K/V store, eliminating the volume reload/commit dance
for ~all of Bazel's day-to-day cache traffic. Only blobs over 4 MiB hit the
volume.

## Setup

```bash
uv sync --extra dev          # install runtime + grpcio-tools
./scripts/gen_protos.sh      # generate Python bindings into modal_rbe/_proto/
modal token new              # one-time, if you haven't authed
```

## Run the server

Two modes:

### Local-side server (cache + executor proxy)

The gRPC server runs on the developer's laptop and proxies every RPC into
Modal. Bazel hits `grpc://localhost:50051`. Each Modal API call costs ~40 ms
RTT, so this is mostly useful for local iteration where the laptop is the
client.

```bash
uv run modal run -m modal_rbe.server::serve
```

### In-cluster server (recommended for performance)

The gRPC server runs *inside* a Modal container so every Dict / Volume /
Function call is in-cluster (sub-ms). The container is exposed to the public
internet via `modal.forward(..., h2_enabled=True)` and protected by a Bearer
token printed at startup.

```bash
uv run modal run -m modal_rbe.server::serve_remote
```

The function prints something like:

```
RBE backend listening at https://<random>.modal.host
Configure Bazel with:
    --remote_cache=grpcs://<random>.modal.host
    --remote_executor=grpcs://<random>.modal.host
    --remote_header=authorization=Bearer <token>
```

Drop the three flags into your workspace's `.bazelrc` (the
`--remote_header` value must be quoted because of the space):

```
build --remote_cache=grpcs://<random>.modal.host
build --remote_executor=grpcs://<random>.modal.host
build --remote_header=authorization="Bearer <token>"
build --remote_instance_name=default
build --remote_timeout=300
```

To reuse a stable token across redeploys, set `MODAL_RBE_AUTH_TOKEN` in the
container environment instead of letting `serve_remote` mint one.

Flags:

| Flag | Default | Notes |
|---|---|---|
| `--port` | `50051` | gRPC port to bind on `localhost`. |
| `--max-workers` | `32` | gRPC server thread pool size. |
| `--exec-enabled` | `true` | Set to `false` to run as a cache-only proxy. |
| `--max-blob-size` | `4 GiB` | Single ByteStream blob cap (buffered in local RAM). |
| `--max-batch-size` | `4 MiB` | Cap advertised in `GetCapabilities` for batched RPCs. |

## Point Bazel at it

Add to your workspace's `.bazelrc`:

```
build --remote_cache=grpc://localhost:50051
build --remote_executor=grpc://localhost:50051   # omit for cache-only mode
build --remote_instance_name=default
build --remote_timeout=300
```

For cache-only use:

```bash
uv run modal run -m modal_rbe.server::serve --no-exec-enabled
```

and only set `--remote_cache` in `.bazelrc`.

## Execution image

The executor image is defined in `modal_rbe/app.py` as `exec_image`. It is the
single image every action runs in — there is no per-action image selection in
v1. Edit the image to add the toolchains your actions need (Bazel sets a
`Platform` proto on each action, but v1 ignores it):

```python
exec_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("build-essential", "git", "curl", "python3", "unzip")
    .pip_install("protobuf>=4.25", "grpcio>=1.60")
    .add_local_python_source("modal_rbe")
)
```

## Layout

```
modal_rbe/
├── app.py            # modal.App, Volume + Dict defs, image definitions
├── digest.py         # sha256 + path helpers shared between local and remote
├── cas.py            # Hybrid CAS dispatch (Dict for small, Volume for large)
├── execute.py        # execute_action Modal Function
├── server.py         # local_entrypoint that boots the gRPC server
├── resource_name.py  # ByteStream resource-name parser
├── servicers/        # one file per gRPC service
│   ├── capabilities.py
│   ├── cas_servicer.py
│   ├── ac_servicer.py     # AC reads/writes go directly against ac_dict
│   ├── bytestream.py
│   └── execution.py
└── _proto/           # generated, gitignored
```

## Limitations (v1)

- ByteStream uploads are buffered in local memory before being shipped to
  Modal — capped at `--max-blob-size`.
- All actions run in one fixed `exec_image`. Per-action `Platform`
  `container-image` hints are ignored.
- No partial-upload resume (`QueryWriteStatus` returns `NOT_FOUND`).
- No support for compressed blobs (`compressed-blobs/...` resource names are
  rejected).
- Single instance only — `instance_name` is not validated, treat as
  single-tenant.

## Regenerating protos

Vendored sources are pinned at the top of `scripts/gen_protos.sh`. To bump:

1. Update `protos/` from upstream.
2. Update commit refs in `scripts/gen_protos.sh`.
3. Re-run `./scripts/gen_protos.sh`.
