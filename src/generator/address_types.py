"""Customer address types and random 1–5 address generation."""

from __future__ import annotations

import random

ADDRESS_TYPES = ("home", "work", "mailing", "billing", "branch")

CITIES = (
    "Istanbul",
    "Ankara",
    "Izmir",
    "Berlin",
    "Munich",
    "Hamburg",
    "Paris",
    "Amsterdam",
    "Vienna",
    "Brussels",
)


def random_address_count() -> int:
    """Each customer must have between 1 and 5 addresses (inclusive)."""
    return random.randint(1, 5)


def random_address_types(count: int | None = None) -> list[str]:
    n = count if count is not None else random_address_count()
    n = max(1, min(5, n))
    return random.sample(list(ADDRESS_TYPES), n)
