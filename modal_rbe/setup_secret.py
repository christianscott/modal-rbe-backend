"""Bootstrap the Modal Secret that holds the bearer token used by
``serve_remote``. Run with::

    uv run python -m modal_rbe.setup_secret           # mint a random token
    uv run python -m modal_rbe.setup_secret <token>   # use a specific token

This is a standalone script — it does NOT load the main `modal_rbe` app, so
it works even when the secret doesn't exist yet (which would otherwise cause
the app's `secrets=[...]` reference to fail to hydrate).
"""

from __future__ import annotations

import secrets
import sys

import modal

SECRET_NAME = "rbe-auth-token"
SECRET_KEY = "MODAL_RBE_AUTH_TOKEN"


def main(token: str | None = None) -> None:
    if not token:
        token = secrets.token_urlsafe(24)
    # Idempotent: delete if it exists, then recreate. `objects.create` doesn't
    # support overwrite-with-new-values; `allow_existing=True` would silently
    # ignore the new token.
    try:
        modal.Secret.objects.delete(SECRET_NAME)
    except Exception:
        pass
    modal.Secret.objects.create(SECRET_NAME, {SECRET_KEY: token})
    print(f"Stored bearer token in Modal Secret '{SECRET_NAME}'.")
    print()
    print("Add to your workspace .bazelrc:")
    print(f'    build --remote_header=authorization="Bearer {token}"')


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
