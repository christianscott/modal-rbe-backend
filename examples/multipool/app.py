"""Example deployment built on `modal_rbe.core`.

Same wire protocol as the canonical `modal_rbe.{app,execute,server}`
deployment, but with a custom set of executor pools. Copy this file into
your project, edit the pool list, then:

    uv run modal deploy -m examples.multipool.app

Bazel selects which pool an action lands in via the `Pool` exec_property:

    build --remote_default_exec_properties=Pool=node

    nodejs_test(
        name = "ts_e2e",
        ...,
        exec_properties = {"Pool": "playwright"},
    )
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading

import modal
import modal.experimental

from modal_rbe.core import (
    default_image_base,
    execute_action_impl,
    make_cache_store,
    serve_forever,
)

APP_NAME = "rbe-multipool-example"
PROXY_REGION = "us-east"
PORT = 50051

app = modal.App(APP_NAME)

cas_volume = modal.Volume.from_name("rbe-multipool-cas", create_if_missing=True)
cas_dict = modal.Dict.from_name("rbe-multipool-cas-small", create_if_missing=True)
ac_dict = modal.Dict.from_name("rbe-multipool-ac", create_if_missing=True)
auth_secret = modal.Secret.from_name(
    "rbe-auth-token", required_keys=["MODAL_RBE_AUTH_TOKEN"]
)

# ---------------------------------------------------------------------------
# Per-pool images. `add_local_python_source("modal_rbe")` ships the action
# body into each container so the executor can `import modal_rbe.core`.
# ---------------------------------------------------------------------------

cache_image = default_image_base().add_local_python_source("modal_rbe")

default_image = (
    default_image_base()
    .apt_install("build-essential", "git", "curl", "ca-certificates", "python3", "unzip")
    .add_local_python_source("modal_rbe")
)

node_image = (
    default_image_base()
    .apt_install("git", "curl", "ca-certificates")
    .run_commands(
        "curl -fsSL https://deb.nodesource.com/setup_22.x | bash -",
        "apt-get install -y nodejs",
    )
    .add_local_python_source("modal_rbe")
)

# A heavyweight pool — playwright deps for browser-based tests.
playwright_image = (
    default_image_base()
    .apt_install("git", "curl")
    .run_commands(
        "curl -fsSL https://deb.nodesource.com/setup_22.x | bash -",
        "apt-get install -y nodejs",
        "npx --yes playwright install --with-deps chromium",
    )
    .add_local_python_source("modal_rbe")
)

# ---------------------------------------------------------------------------
# Cache store — registers volume-side helpers on `app`.
# ---------------------------------------------------------------------------

cache = make_cache_store(
    app=app,
    cache_image=cache_image,
    cas_volume=cas_volume,
    cas_dict=cas_dict,
    ac_dict=ac_dict,
    region=PROXY_REGION,
)

# ---------------------------------------------------------------------------
# Per-pool executor functions. Each is a thin wrapper around
# `execute_action_impl`.
# ---------------------------------------------------------------------------

EXEC_TIMEOUT = 3600
EXEC_MAX_CONTAINERS = 4
EXEC_INPUT_CONCURRENCY = 4


def _executor_kwargs(image, *, min_containers=0):
    return dict(
        image=image,
        volumes={cache.cas_mount: cas_volume},
        timeout=EXEC_TIMEOUT,
        min_containers=min_containers,
        max_containers=EXEC_MAX_CONTAINERS,
        region=PROXY_REGION,
    )


@app.function(**_executor_kwargs(default_image, min_containers=1))
@modal.concurrent(max_inputs=EXEC_INPUT_CONCURRENCY)
def execute_default(action_hash: str, action_size: int) -> bytes:
    return execute_action_impl(action_hash, action_size, cache=cache)


@app.function(**_executor_kwargs(node_image))
@modal.concurrent(max_inputs=EXEC_INPUT_CONCURRENCY)
def execute_node(action_hash: str, action_size: int) -> bytes:
    return execute_action_impl(action_hash, action_size, cache=cache)


@app.function(**_executor_kwargs(playwright_image))
@modal.concurrent(max_inputs=2)  # heavier per-action; lower fan-out
def execute_playwright(action_hash: str, action_size: int) -> bytes:
    return execute_action_impl(action_hash, action_size, cache=cache)


POOLS: dict[str, modal.Function] = {
    "default": execute_default,
    "node": execute_node,
    "playwright": execute_playwright,
}

# ---------------------------------------------------------------------------
# gRPC server class behind Modal's Flash proxy.
# ---------------------------------------------------------------------------


@app.cls(
    image=cache_image,
    secrets=[auth_secret],
    min_containers=1,
    max_containers=1,
    timeout=24 * 60 * 60,
    region=PROXY_REGION,
)
@modal.experimental.http_server(
    port=PORT, proxy_regions=[PROXY_REGION], h2_enabled=True
)
class RbeServer:
    @modal.enter()
    def start(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        token = os.environ["MODAL_RBE_AUTH_TOKEN"]

        def _run() -> None:
            try:
                asyncio.run(
                    serve_forever(
                        cache=cache,
                        pools=POOLS,
                        auth_token=token,
                        port=PORT,
                    )
                )
            except Exception:
                logging.exception("gRPC server crashed; container will exit")
                os._exit(1)

        threading.Thread(target=_run, daemon=False, name="rbe-grpc").start()
