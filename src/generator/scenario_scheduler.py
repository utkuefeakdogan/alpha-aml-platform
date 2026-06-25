"""Random-wake scenario scheduler — daily alert budget + weekly rule coverage."""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.generator.customer_loader import count_flags_last_24h, flags_by_rule_last_7d

logger = logging.getLogger(__name__)

WAKE_MIN_SEC = float(os.getenv("SCENARIO_WAKE_MIN_SEC", "1800"))
WAKE_MAX_SEC = float(os.getenv("SCENARIO_WAKE_MAX_SEC", "7200"))
WAKE_JITTER_SEC = float(os.getenv("SCENARIO_WAKE_JITTER_SEC", "600"))
DAILY_CAP = int(os.getenv("SCENARIO_DAILY_CAP", "100"))
RULE_COOLDOWN_SEC = float(os.getenv("SCENARIO_RULE_COOLDOWN_SEC", "10800"))
CATALOG_PATH = Path(os.getenv("SCENARIO_CATALOG_PATH", "/app/configs/scenario_catalog.json"))


def _load_catalog(path: Path) -> list[dict]:
    with open(path) as f:
        data = json.load(f)
    scenarios = data.get("scenarios", data if isinstance(data, list) else [])
    return [s for s in scenarios if s.get("enabled", True)]


class ScenarioScheduler:
    def __init__(self, catalog_path: Path | None = None) -> None:
        path = catalog_path or CATALOG_PATH
        self._scenarios = _load_catalog(path)
        random.shuffle(self._scenarios)
        self._next_wake: datetime | None = None
        self._last_inject_by_rule: dict[str, datetime] = {}
        self._schedule_next_wake()

    def _schedule_next_wake(self) -> None:
        gap = random.uniform(WAKE_MIN_SEC, WAKE_MAX_SEC) + random.uniform(0, WAKE_JITTER_SEC)
        self._next_wake = datetime.now(timezone.utc) + timedelta(seconds=gap)
        logger.info("Next scenario wake scheduled at %s (+%.0fs)", self._next_wake.isoformat(), gap)

    def due(self) -> bool:
        if not self._scenarios:
            return False
        if count_flags_last_24h() >= DAILY_CAP:
            return False
        now = datetime.now(timezone.utc)
        return self._next_wake is not None and now >= self._next_wake

    def _eligible(self, scenario: dict, now: datetime) -> bool:
        rule = scenario["rule_name"]
        last = self._last_inject_by_rule.get(rule)
        if last and (now - last).total_seconds() < RULE_COOLDOWN_SEC:
            return False
        return True

    def pick_scenario(self) -> dict | None:
        if not self._scenarios:
            return None
        now = datetime.now(timezone.utc)
        counts_7d = flags_by_rule_last_7d()

        # Hafta içinde henüz alert üretmemiş kurallara öncelik
        missing_week = [
            s for s in self._scenarios if counts_7d.get(s["rule_name"], 0) == 0
        ]
        if missing_week:
            pool = [s for s in missing_week if self._eligible(s, now)]
            if not pool:
                pool = missing_week
        else:
            eligible = [s for s in self._scenarios if self._eligible(s, now)]
            pool = eligible or list(self._scenarios)
            pool.sort(key=lambda s: counts_7d.get(s["rule_name"], 0))
            pool = pool[:3]

        weights = [float(s.get("priority_weight", 1.0)) for s in pool]
        chosen = random.choices(pool, weights=weights, k=1)[0]
        self._last_inject_by_rule[chosen["rule_name"]] = now
        self._schedule_next_wake()
        logger.info(
            "Picked scenario %s (7d flags=%s)",
            chosen["rule_name"],
            counts_7d.get(chosen["rule_name"], 0),
        )
        return chosen

    def enabled_count(self) -> int:
        return len(self._scenarios)
