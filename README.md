# modal-rbe-backend

A Bazel Remote Execution / Remote Cache backend deployed entirely on
[Modal](https://modal.com). The gRPC server runs **inside** a Modal container,
exposed to the public internet over HTTPS+H2 via `modal.forward`. Cache state
is split between a `modal.Dict` (hot path) and a `modal.Volume` (large blobs).
Actions execute in a separate Modal Function with a per-container hardlink
pool to amortize input materialization.

```
 Bazel ──gRPCS──▶ modal.forward tunnel ──▶ RbeServer (@app.cls, in Modal)
                                              │
                              ┌───────────────┼─────────────────────────┐
                              ▼               ▼                         ▼
                       ac_dict (Dict)   cas_dict (Dict)        cas_volume (Volume)
                        ActionResults   ≤32 MiB blobs           >32 MiB blobs
                                                                       │
                              ┌────────────────────────────────────────┘
                              ▼
                       execute_action (Modal Function, @modal.concurrent)
                       — fixed exec_image
                       — /cas-pool hardlink pool (per-container)
```

The Dict is the authoritative presence map for CAS — `FindMissingBlobs` is a
pure in-memory check against a bounded LRU mirror, never touching the volume.
Volume reads only happen for blobs over 32 MiB. AC reads are served from a
bounded LRU value cache, falling back to the Dict on miss.

## Setup

```bash
uv sync --extra dev          # install runtime + grpcio-tools
./scripts/gen_protos.sh      # generate Python bindings into modal_rbe/_proto/
modal token new              # one-time, if you haven't authed
```

## Deploy

```bash
# 1. Bootstrap the bearer-token Secret (one-time):
uv run python -m modal_rbe.setup_secret           # mint a random token
uv run python -m modal_rbe.setup_secret <token>   # or use a specific token

# 2. Deploy the app:
uv run modal deploy -m modal_rbe.server

# 3. Fetch the current tunnel URL (regenerated on each container restart):
uv run python -m modal_rbe.url            # grpcs://...modal.host
uv run python -m modal_rbe.url --bazelrc  # ready-to-paste .bazelrc snippet
```

The deployed app auto-starts via `@modal.enter()` (no separate `modal run`
invocation needed) and stays alive thanks to `min_containers=1`. The Bearer
token is stable across redeploys — it lives in the `rbe-auth-token` Modal
Secret. The URL is dynamic per container boot, so re-run `python -m
modal_rbe.url` after a redeploy or container recycle.

## Point Bazel at it

Drop the URL + token into your workspace's `.bazelrc` (the `--remote_header`
value must be quoted because of the embedded space):

```
build --remote_cache=grpcs://<URL>.w.modal.host
build --remote_executor=grpcs://<URL>.w.modal.host
build --remote_header=authorization="Bearer <TOKEN>"
build --remote_instance_name=default
build --remote_timeout=300
```

Or omit `--remote_executor` to use it as a remote cache only. See
`examples/hello/.bazelrc.local.example` and
`examples/exec-go/.bazelrc.local.example` for templates.

## Examples

- `examples/hello/` — three trivial genrules. Smoke test for the cache
  plane and execution. With `--remote_executor`, all three run on Modal.
- `examples/exec-go/` — a pure-Go binary built with `rules_go` targeting
  `linux_amd64`. Forces every Go toolchain action (compile, link, stdlib)
  onto the Modal executor; the local darwin host can't execute the linux
  toolchain binaries. Useful for benchmarking remote execution latency.

## Performance shape

Cached rebuild of `bazel-remote` (309 cache hits, 1 local link):

- ~3.9 s elapsed, ~2.2 s critical path
- Critical path is dominated by the local `GoLink` and the Bazel→Modal RTT
  (~30 ms per AC fetch on the chain)

Incremental Go compile on `examples/exec-go` (1 source file changed,
2 fresh remote actions):

- ~6 s end-to-end after the executor's input pool has warmed up
- First build after a container recycle is closer to ~20 s — the pool is
  rebuilt from cold (Modal has no per-machine persistent fast disk)

## Architecture details worth knowing

### Cache plane

- **`cas_dict` ("rbe-cas-small")** holds blobs ≤ 32 MiB inline with a 1-byte
  tag (`\x00` = inline bytes, `\x01` = "look on the volume"). It's the
  authoritative presence map; `find_missing` consults a bounded
  in-process LRU mirror that's a strict subset of the Dict (false-missing
  is harmless, false-present impossible).
- **`cas_volume` ("rbe-cas")** stores blobs > 32 MiB. Reads use
  optimistic-then-reload (`open` first, only call `volume.reload()` on
  `FileNotFoundError`).
- **`ac_dict` ("rbe-ac")** holds serialized `ActionResult` protos. A bounded
  in-process LRU value cache fronts it; `GetActionResult` serves from
  memory and falls back to the Dict on miss.
- All RPC fan-out (e.g. `BatchReadBlobs`) is gated by a 64-permit
  semaphore so we don't crush Modal's grpclib transport.

### Execution plane

- `execute_action` runs in a separate `@app.function` with `exec_image`
  mounted with `cas_volume`.
- `min_containers=1`, `max_containers=4`, `@modal.concurrent(max_inputs=4)`.
  One container handles up to 4 actions in parallel; up to 4 containers
  can run if Bazel fans out widely.
- Modal gives every container **512 GiB of SSD-backed scratch by default**
  (`ephemeral_disk`); the hardlink pool lives on it. 512 GiB is also
  Modal's *minimum* — you can't request smaller.
- **Per-container hardlink pool at `/cas-pool/<hash[:2]>/<hash>`.** Each
  blob is materialized once on first reference; subsequent actions on the
  same container hardlink from the pool into per-action workspaces via
  `os.link` — kernel-level, near-free.
- Pool inode mode is `0o755` so the executor never has to `chmod` a
  hardlink (which would alias the inode mode across every path pointing
  at it).

### Auth

- Every gRPC method is gated by a `_BearerAuthInterceptor` that requires
  `authorization: Bearer <token>` matching the `rbe-auth-token` Modal
  Secret. Unauthenticated requests are short-circuited to
  `UNAUTHENTICATED` before any request bytes are deserialized.

## Layout

```
modal_rbe/
├── app.py            # modal.App, Volume + Dict defs, image definitions
├── digest.py         # sha256 + path helpers shared between local and remote
├── lru.py            # BoundedLruSet / BoundedLruDict (in-process caches)
├── cas.py            # Hybrid CAS dispatch (Dict for small, Volume for large)
├── execute.py        # execute_action Modal Function + hardlink pool
├── server.py         # @app.cls RbeServer + auth interceptor
├── resource_name.py  # ByteStream resource-name parser
├── setup_secret.py   # one-time bootstrap of rbe-auth-token Secret
├── url.py            # CLI to fetch the deployed tunnel URL
├── telemetry.py      # per-RPC timing snapshots (printed every 2 s)
├── servicers/        # one file per gRPC service
│   ├── capabilities.py
│   ├── cas_servicer.py
│   ├── ac_servicer.py
│   ├── bytestream.py
│   └── execution.py
└── _proto/           # generated, gitignored
```

## Limitations

- **No persistent fast local disk across container restarts.** The
  executor's `ephemeral_disk` is SSD-backed and per-container, so the
  hardlink pool is fast — but it's wiped whenever Modal recycles the
  container. `min_containers=1` keeps recycles infrequent in practice.
  Modal doesn't expose anything that survives recycle (e.g. an attached
  NVMe scoped to the worker).
- **Single executor image.** Every action runs in `exec_image`. Per-action
  `Platform.container-image` hints are ignored.
- **Wide-fanout builds top out at one container's vCPUs.** With
  `max_inputs=4` and `max_containers=4`, the executor can chew through 16
  parallel actions; past that, additional actions queue. Trading
  `max_containers` higher gives parallel cold pools — a tradeoff between
  fan-out and warmth.
- **Pool aliases inode mode.** Non-executable inputs appear with `+x` set
  because every pool inode is `0o755`. Bazel doesn't typically execute its
  inputs, so the practical risk is low.
- **No partial-upload resume.** `QueryWriteStatus` returns `NOT_FOUND`;
  Bazel restarts the upload.
- **No compressed blobs.** Resource names with `compressed-blobs/...` are
  rejected.
- **Single instance, no auth scoping.** `instance_name` isn't validated;
  the bearer token is the only access control.

## Regenerating protos

Vendored sources are pinned at the top of `scripts/gen_protos.sh`. To bump:

1. Update `protos/` from upstream.
2. Update commit refs in `scripts/gen_protos.sh`.
3. Re-run `./scripts/gen_protos.sh`.
