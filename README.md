# modal-rbe-backend

A Bazel Remote Execution / Remote Cache backend deployed entirely on
[Modal](https://modal.com). The gRPC server runs **inside** a Modal container,
exposed to the public internet over HTTPS+H2 via Modal's experimental Flash
HTTP/2 edge proxy (`modal.experimental.http_server`). Cache state is split
between a `modal.Dict` (hot path) and a `modal.Volume` (large blobs).
Actions execute in a separate Modal Function with a per-container hardlink
pool to amortize input materialization.

```
 Bazel ──gRPCS──▶ Modal Flash proxy ──▶ RbeServer (@app.cls, in Modal)
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
```

`modal deploy` prints the stable Flash endpoint — something like

    https://<workspace>--rbe-backend-rbeserver.us-east.modal.direct

The endpoint is bound to the app + class name and stays valid across
redeploys and container recycles, so paste it into your `.bazelrc.local`
once. The Bearer token is also stable across redeploys (it lives in the
`rbe-auth-token` Modal Secret).

## Point Bazel at it

Drop the URL + token into your workspace's `.bazelrc` (the `--remote_header`
value must be quoted because of the embedded space):

```
build --remote_cache=grpcs://<workspace>--rbe-backend-rbeserver.us-east.modal.direct
build --remote_executor=grpcs://<workspace>--rbe-backend-rbeserver.us-east.modal.direct
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

- One Modal Function per **action pool**. A pool = an Image plus per-pool
  Function options (e.g. `min_containers`). The default pool is
  `debian_slim + build-essential + git + python3` and is always warm
  (`min_containers=1`); the `light` pool is bare `debian_slim` and scales
  to zero. Add a pool by declaring an Image in `app.py`, registering a
  thin wrapper Function in `execute.py`, and adding it to
  `EXECUTORS_BY_POOL`.
- Bazel chooses the pool via the `Pool` exec_property:

  ```
  # workspace-wide
  build --remote_default_exec_properties=Pool=light

  # per-target
  cc_binary(
      name = "fast",
      srcs = ["main.c"],
      exec_properties = {"Pool": "light"},
  )
  ```

  Actions without a `Pool` property (or with an unknown pool name) route
  to `default` and the unknown-pool case is logged.
- `max_containers=4`, `@modal.concurrent(max_inputs=4)` per pool. One
  container handles up to 4 actions in parallel; up to 4 containers can
  run if Bazel fans out widely (so 16 in-flight per pool).
- Modal gives every container **512 GiB of SSD-backed scratch by default**
  (`ephemeral_disk`); the hardlink pool lives on it. 512 GiB is also
  Modal's *minimum* — you can't request smaller.
- **Per-container hardlink pool at `/cas-pool/<hash[:2]>/<hash>`.** Each
  blob is materialized once on first reference; subsequent actions on the
  same container hardlink from the pool into per-action workspaces via
  `os.link` — kernel-level, near-free. Each action pool has its own
  hardlink pool; no cross-pool sharing.
- Pool inode mode is `0o755` so the executor never has to `chmod` a
  hardlink (which would alias the inode mode across every path pointing
  at it).

### Auth

- Every gRPC method is gated by a `_BearerAuthInterceptor` that requires
  `authorization: Bearer <token>` matching the `rbe-auth-token` Modal
  Secret. Unauthenticated requests are short-circuited to
  `UNAUTHENTICATED` before any request bytes are deserialized.

## Layout

The package splits into a reusable **library** (`modal_rbe.core`) and a
specific **deployment** that uses it (`modal_rbe.{app,execute,server}`):

```
modal_rbe/
├── core/                    # reusable library — no decorators at module scope
│   ├── cache.py             #   CacheStore + make_cache_store()
│   ├── execute_impl.py      #   execute_action_impl() + hardlink pool
│   ├── grpc_server.py       #   build_grpc_server() + auth interceptor
│   ├── images.py            #   default_image_base()
│   ├── digest.py            #   sha256 + path helpers
│   ├── lru.py               #   BoundedLruSet / BoundedLruDict
│   ├── resource_name.py     #   ByteStream resource-name parser
│   ├── telemetry.py         #   per-RPC timing snapshots
│   └── servicers/           #   parametrized gRPC servicers
│       ├── capabilities.py
│       ├── cas_servicer.py
│       ├── ac_servicer.py
│       ├── bytestream.py
│       └── execution.py
├── app.py                   # default deployment: App, images, CacheStore
├── execute.py               # default deployment: per-pool @app.function
├── server.py                # default deployment: @app.cls RbeServer
├── setup_secret.py          # one-time bootstrap of rbe-auth-token Secret
└── _proto/                  # generated, gitignored

examples/
├── hello/                   # genrule smoke test
├── exec-go/                 # cross-compile demo
└── multipool/               # custom deployment with default + node + playwright pools
```

### Building a custom deployment

To run a different set of pools (different toolchains, different region,
different resource caps), don't edit the canonical `modal_rbe.{app,execute,
server}`. Instead write your own `app.py` against `modal_rbe.core`:

```python
import modal, modal.experimental
from modal_rbe.core import (
    default_image_base, make_cache_store,
    execute_action_impl, serve_forever,
)

app = modal.App("my-rbe")
cas_volume = modal.Volume.from_name("rbe-cas", create_if_missing=True)
# ... etc

cache_image = default_image_base().add_local_python_source("modal_rbe")
cache = make_cache_store(
    app=app, cache_image=cache_image,
    cas_volume=cas_volume, cas_dict=cas_dict, ac_dict=ac_dict,
    region="us-east",
)

@app.function(image=my_image, volumes={cache.cas_mount: cas_volume}, ...)
@modal.concurrent(max_inputs=4)
def execute_my_pool(h, sz):
    return execute_action_impl(h, sz, cache=cache)

POOLS = {"default": execute_my_pool, ...}

@app.cls(...)
@modal.experimental.http_server(port=50051, h2_enabled=True, ...)
class RbeServer:
    @modal.enter()
    def start(self):
        token = os.environ["MODAL_RBE_AUTH_TOKEN"]
        threading.Thread(
            target=lambda: asyncio.run(serve_forever(
                cache=cache, pools=POOLS, auth_token=token, port=50051,
            )),
            daemon=False,
        ).start()
```

See `examples/multipool/app.py` for a full worked example.

## Limitations

- **No persistent fast local disk across container restarts.** The
  executor's `ephemeral_disk` is SSD-backed and per-container, so the
  hardlink pool is fast — but it's wiped whenever Modal recycles the
  container. `min_containers=1` keeps recycles infrequent in practice.
  Modal doesn't expose anything that survives recycle (e.g. an attached
  NVMe scoped to the worker).
- **Pre-defined executor pools.** Each pool is one Image bound at deploy
  time; arbitrary registry images requested via Bazel's `container-image`
  exec_property are not honored. Adding a pool requires a code change +
  redeploy. See "Execution plane" above.
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
