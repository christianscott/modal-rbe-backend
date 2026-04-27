"""Tiny bounded-LRU helpers.

Single-threaded usage assumed (one asyncio loop in the gRPC server thread);
no locking. Backed by ``collections.OrderedDict`` for O(1) insert / lookup /
move-to-end.
"""

from __future__ import annotations

from collections import OrderedDict


class BoundedLruSet:
    """A bounded LRU set of hashable items.

    `add` and `__contains__` both refresh the entry's recency. When the set
    grows past `maxsize`, the least-recently-used item is evicted.
    """

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._max = maxsize
        self._d: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> None:
        if key in self._d:
            self._d.move_to_end(key)
            return
        self._d[key] = None
        while len(self._d) > self._max:
            self._d.popitem(last=False)

    def __contains__(self, key: object) -> bool:
        if key in self._d:
            self._d.move_to_end(key)  # refresh recency on hit
            return True
        return False

    def __len__(self) -> int:
        return len(self._d)


class BoundedLruDict:
    """A bounded LRU mapping. Eviction by entry count, not byte size."""

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._max = maxsize
        self._d: OrderedDict = OrderedDict()

    def get(self, key, default=None):
        if key in self._d:
            self._d.move_to_end(key)
            return self._d[key]
        return default

    def put(self, key, value) -> None:
        if key in self._d:
            self._d[key] = value
            self._d.move_to_end(key)
            return
        self._d[key] = value
        while len(self._d) > self._max:
            self._d.popitem(last=False)

    def __contains__(self, key: object) -> bool:
        return key in self._d

    def __len__(self) -> int:
        return len(self._d)
