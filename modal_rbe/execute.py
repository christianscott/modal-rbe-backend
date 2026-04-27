"""Default deployment: per-pool executor functions.

Each `@app.function` here is a thin wrapper around `execute_action_impl`
(from `modal_rbe.core`). The wrapper picks the toolchain image; the impl
does materialize / run / collect.

Add a pool by:
  1. defining its image in `app.py`
  2. registering a wrapper here
  3. adding it to `EXECUTORS_BY_POOL`
"""

from __future__ import annotations

import modal

from .app import app, cache, cas_volume, default_exec_image, light_exec_image
from .core.execute_impl import execute_action_impl

EXEC_FUNCTION_TIMEOUT = 3600
_EXEC_MAX_CONTAINERS = 4
_EXEC_INPUT_CONCURRENCY = 4
_EXEC_REGION = "us-east"


@app.function(
    image=default_exec_image,
    volumes={cache.cas_mount: cas_volume},
    timeout=EXEC_FUNCTION_TIMEOUT,
    min_containers=1,
    max_containers=_EXEC_MAX_CONTAINERS,
    region=_EXEC_REGION,
)
@modal.concurrent(max_inputs=_EXEC_INPUT_CONCURRENCY)
def execute_default(action_hash: str, action_size: int) -> bytes:
    """Default pool — debian_slim + build-essential + git + python3."""
    return execute_action_impl(action_hash, action_size, cache=cache)


@app.function(
    image=light_exec_image,
    volumes={cache.cas_mount: cas_volume},
    timeout=EXEC_FUNCTION_TIMEOUT,
    # min_containers defaults to 0: scale to zero when no `Pool=light` traffic.
    max_containers=_EXEC_MAX_CONTAINERS,
    region=_EXEC_REGION,
)
@modal.concurrent(max_inputs=_EXEC_INPUT_CONCURRENCY)
def execute_light(action_hash: str, action_size: int) -> bytes:
    """Light pool — bare debian_slim. For actions whose command only needs
    a shell + python interpreter."""
    return execute_action_impl(action_hash, action_size, cache=cache)


EXECUTORS_BY_POOL: dict[str, modal.Function] = {
    "default": execute_default,
    "light": execute_light,
}
