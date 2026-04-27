"""Default deployment: shared resources.

This file plus `execute.py` and `server.py` are one specific deployment
built on top of `modal_rbe.core`. Copy these three files (and adjust the
images, region, pool list, etc.) to spin up a different deployment with
its own pool configuration.
"""

from __future__ import annotations

import modal

from .core.cache import make_cache_store
from .core.images import default_image_base

APP_NAME = "rbe-backend"
PROXY_REGION = "us-east"
PORT = 50051

app = modal.App(APP_NAME)

cas_volume = modal.Volume.from_name("rbe-cas", create_if_missing=True)
cas_dict = modal.Dict.from_name("rbe-cas-small", create_if_missing=True)
ac_dict = modal.Dict.from_name("rbe-ac", create_if_missing=True)

# Auth token Modal Secret. Bootstrap with `python -m modal_rbe.setup_secret`.
auth_secret = modal.Secret.from_name(
    "rbe-auth-token", required_keys=["MODAL_RBE_AUTH_TOKEN"]
)

# ---------------------------------------------------------------------------
# Per-deployment images. Add toolchain bits on top of `default_image_base()`,
# then add your local Python source so the executor can import the action
# body (`execute_action_impl`) and the cache helpers.
# ---------------------------------------------------------------------------

cache_image = default_image_base().add_local_python_source("modal_rbe")

default_exec_image = (
    default_image_base()
    .apt_install(
        "build-essential",
        "git",
        "curl",
        "ca-certificates",
        "python3",
        "unzip",
    )
    .add_local_python_source("modal_rbe")
)

light_exec_image = default_image_base().add_local_python_source("modal_rbe")

# ---------------------------------------------------------------------------
# CacheStore: registers the volume-side helpers on this app and bundles
# every storage handle the executor + servicers need.
# ---------------------------------------------------------------------------

cache = make_cache_store(
    app=app,
    cache_image=cache_image,
    cas_volume=cas_volume,
    cas_dict=cas_dict,
    ac_dict=ac_dict,
    region=PROXY_REGION,
)
