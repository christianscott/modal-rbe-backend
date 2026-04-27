"""Default deployment: the gRPC-server class behind Modal's Flash proxy.

Run with:

    uv run modal deploy -m modal_rbe.server
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading

import modal_rbe  # noqa: F401  (side-effecting: registers _proto path)

import modal
import modal.experimental

from .app import app, auth_secret, cache, cache_image, PORT, PROXY_REGION
from .core.grpc_server import serve_forever
# Side-effecting: importing this module registers per-pool @app.function
# executors on `app`.
from .execute import EXECUTORS_BY_POOL

log = logging.getLogger("modal_rbe")

SERVE_TIMEOUT = 24 * 60 * 60
_AUTH_SECRET_KEY = "MODAL_RBE_AUTH_TOKEN"


@app.cls(
    image=cache_image,
    secrets=[auth_secret],
    min_containers=1,
    max_containers=1,
    timeout=SERVE_TIMEOUT,
    region=PROXY_REGION,
)
@modal.experimental.http_server(
    port=PORT,
    proxy_regions=[PROXY_REGION],
    h2_enabled=True,
)
class RbeServer:
    @modal.enter()
    def start(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        token = os.environ[_AUTH_SECRET_KEY]

        def _run() -> None:
            try:
                asyncio.run(
                    serve_forever(
                        cache=cache,
                        pools=EXECUTORS_BY_POOL,
                        auth_token=token,
                        port=PORT,
                    )
                )
            except Exception:  # noqa: BLE001
                log.exception("gRPC server crashed; container will exit")
                os._exit(1)

        # daemon=False keeps the process alive for the container's lifetime.
        threading.Thread(target=_run, daemon=False, name="rbe-grpc").start()
