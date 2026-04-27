from __future__ import annotations

import contextlib
import time
from collections import defaultdict
from threading import Lock

_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "total": 0.0})
_lock = Lock()


@contextlib.contextmanager
def timed(name: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        with _lock:
            s = _stats[name]
            s["count"] += 1
            s["total"] += dt


def snapshot_and_reset() -> dict[str, dict[str, float]]:
    with _lock:
        snap = {k: dict(v) for k, v in _stats.items()}
        _stats.clear()
    return snap
