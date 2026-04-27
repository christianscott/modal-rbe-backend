"""Image factories for building executor / cache-plane images.

Library users typically build executor images by chaining onto
``default_image_base()`` and then ``.add_local_python_source(...)`` of
whatever package holds their app definition.
"""

from __future__ import annotations

import modal


def default_image_base(python_version: str = "3.11") -> modal.Image:
    """Minimal image that includes the wire dependencies the executor and
    cache plane both need (`protobuf`, `grpcio`). Add toolchain-specific
    apt/pip packages on top, then add your local Python source."""
    return (
        modal.Image.debian_slim(python_version=python_version)
        .pip_install("protobuf>=4.25", "grpcio>=1.60")
    )
