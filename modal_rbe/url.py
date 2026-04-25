"""Print the URL of the currently-running ``RbeServer`` deployment.

Run with::

    uv run python -m modal_rbe.url            # prints "grpcs://..."
    uv run python -m modal_rbe.url --bazelrc  # prints a .bazelrc snippet

The URL is published by ``RbeServer.start`` into the
``rbe-server-url`` Modal Dict on each container boot. Right after a
``modal deploy``, give the container a few seconds to come up before reading.
"""

from __future__ import annotations

import sys

import modal

URL_DICT_NAME = "rbe-server-url"
URL_DICT_KEY = "current"


def main() -> None:
    bazelrc = "--bazelrc" in sys.argv[1:]
    d = modal.Dict.from_name(URL_DICT_NAME, create_if_missing=True)
    url = d.get(URL_DICT_KEY, None)
    if not url:
        print(
            "no active RbeServer; run `modal deploy modal_rbe.server` and "
            "wait for the container to come up.",
            file=sys.stderr,
        )
        sys.exit(1)
    if bazelrc:
        print(f"build --remote_cache={url}")
        print(f"build --remote_executor={url}")
        print(
            'build --remote_header=authorization="Bearer '
            "<value of MODAL_RBE_AUTH_TOKEN in the rbe-auth-token Secret>"
            '"'
        )
    else:
        print(url)


if __name__ == "__main__":
    main()
