"""modal_rbe.core — reusable building blocks for a Modal-hosted Bazel RBE.

Typical use from a deployment's `app.py`:

    import modal, modal.experimental
    from modal_rbe.core import (
        default_image_base, make_cache_store,
        execute_action_impl, serve_forever,
    )

    app = modal.App("my-rbe")
    cas_volume = modal.Volume.from_name("rbe-cas", create_if_missing=True)
    cas_dict   = modal.Dict.from_name("rbe-cas-small", create_if_missing=True)
    ac_dict    = modal.Dict.from_name("rbe-ac", create_if_missing=True)
    secret     = modal.Secret.from_name("rbe-auth-token",
                                        required_keys=["MODAL_RBE_AUTH_TOKEN"])

    cache_image = default_image_base().add_local_python_source("modal_rbe", "myapp")
    cache = make_cache_store(
        app=app,
        cache_image=cache_image,
        cas_volume=cas_volume,
        cas_dict=cas_dict,
        ac_dict=ac_dict,
        region="us-east",
    )

    # … then declare per-pool @app.function executors, build the
    # @app.cls server, etc. See examples/ for a complete deployment.
"""

from .cache import CacheStore, EMPTY_HASH, make_cache_store
from .execute_impl import POOL_DIR, execute_action_impl
from .grpc_server import build_grpc_server, serve_forever
from .images import default_image_base
from .servicers.execution import DEFAULT_POOL, POOL_PROPERTY_KEY

__all__ = [
    "CacheStore",
    "DEFAULT_POOL",
    "EMPTY_HASH",
    "POOL_DIR",
    "POOL_PROPERTY_KEY",
    "build_grpc_server",
    "default_image_base",
    "execute_action_impl",
    "make_cache_store",
    "serve_forever",
]
