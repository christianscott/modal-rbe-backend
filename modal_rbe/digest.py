from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DigestKey:
    hash: str
    size: int

    def __str__(self) -> str:
        return f"{self.hash}/{self.size}"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def cas_path(root: str, hash_: str) -> str:
    # Two-char shard to keep any single directory small.
    return os.path.join(root, hash_[:2], hash_)


def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
