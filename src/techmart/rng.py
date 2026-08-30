from __future__ import annotations

import hashlib

import numpy as np


class SeededRng:
    """Factory for independent, reproducible RNG substreams.

    Each named stream is derived deterministically from the base seed, so
    generators for different tables/columns are reproducible run-to-run yet
    statistically independent of one another.
    """

    def __init__(self, base_seed: int) -> None:
        self.base_seed = base_seed

    def stream(self, name: str) -> np.random.Generator:
        digest = hashlib.sha256(f"{self.base_seed}:{name}".encode()).digest()
        seed = int.from_bytes(digest[:8], "big")
        return np.random.default_rng(seed)
