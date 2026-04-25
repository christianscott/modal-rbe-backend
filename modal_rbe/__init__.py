import os
import sys

_PROTO_DIR = os.path.join(os.path.dirname(__file__), "_proto")
if os.path.isdir(_PROTO_DIR) and _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)
