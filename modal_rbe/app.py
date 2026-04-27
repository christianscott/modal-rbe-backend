from __future__ import annotations

import modal

APP_NAME = "rbe-backend"

app = modal.App(APP_NAME)

cas_volume = modal.Volume.from_name("rbe-cas", create_if_missing=True)

# Action Cache: small ActionResult protos keyed by action hash. modal.Dict is
# a strongly-consistent K/V store — way better fit than a Volume for AC.
ac_dict = modal.Dict.from_name("rbe-ac", create_if_missing=True)

# CAS small-blob hot path. Strongly consistent. Bazel's hot CAS traffic (action
# protos, command protos, small source files) all fits here. Anything bigger
# falls through to cas_volume.
cas_dict = modal.Dict.from_name("rbe-cas-small", create_if_missing=True)

CAS_MOUNT = "/cas"

# Image used by all cache-plane Modal Functions. Only needs protobuf for
# serializing/deserializing ActionResult etc.
cache_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("protobuf>=4.25", "grpcio>=1.60")
    .add_local_python_source("modal_rbe")
)

# Default executor image — covers a typical C/C++/Python build. Bazel
# actions land here unless the action's Platform proto specifies a different
# `Pool` exec_property (see modal_rbe/execute.py).
default_exec_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "build-essential",
        "git",
        "curl",
        "ca-certificates",
        "python3",
        "unzip",
    )
    .pip_install("protobuf>=4.25", "grpcio>=1.60")
    .add_local_python_source("modal_rbe")
)

# A trimmed-down executor image with no apt packages — for actions that only
# need the standard interpreter / shell. Demonstrates pool routing; pick
# `Pool=light` from Bazel via exec_properties to land here.
light_exec_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("protobuf>=4.25", "grpcio>=1.60")
    .add_local_python_source("modal_rbe")
)

# Backwards-compatible alias; `exec_image` may still be imported by
# downstream code.
exec_image = default_exec_image
