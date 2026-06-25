"""Load scenario catalog for Rule Builder and KPIs."""

from __future__ import annotations

import json
import os
from pathlib import Path


def catalog_path() -> Path:
    candidates = [
        os.getenv("SCENARIO_CATALOG_PATH", "").strip(),
        "/app/configs/scenario_catalog.json",
        str(Path(__file__).resolve().parents[2] / "configs" / "scenario_catalog.json"),
    ]
    for raw in candidates:
        if not raw:
            continue
        p = Path(raw)
        if p.is_file():
            return p
    return Path(__file__).resolve().parents[2] / "configs" / "scenario_catalog.json"


def load_scenario_catalog() -> list[dict]:
    path = catalog_path()
    if not path.exists():
        return []
    with open(path) as f:
        data = json.load(f)
    scenarios = data.get("scenarios", data if isinstance(data, list) else [])
    return list(scenarios)


def active_scenario_count() -> int:
    return sum(1 for s in load_scenario_catalog() if s.get("enabled", True))


def save_scenario_catalog(scenarios: list[dict]) -> None:
    path = catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"scenarios": scenarios}, f, indent=2)
        f.write("\n")
