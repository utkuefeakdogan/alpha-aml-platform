"""Load and persist AML detection rules (JSON canonical)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

DEFAULT_RULES = {
    "velocity": {"window_minutes": 5, "max_txns_per_account": 5, "min_txns_per_minute": 5},
    "high_value": {"threshold_eur": 10000},
    "geographic": {
        "blocked_countries": ["IR", "KP", "SY", "CU", "RU"],
        "high_risk_countries": ["RU", "KP"],
    },
    "multi_window": {
        "daily_velocity_max": 5,
        "daily_velocity_max_amount_eur": 1000,
        "weekly_volume_max_eur": 10000,
        "biweekly_distinct_receivers_max": 20,
        "monthly_peer_anomaly_multiplier": 2.5,
        "monthly_peer_baseline_txn_count": 8,
    },
    "smurfing": {"weekly_small_txn_threshold_eur": 500, "weekly_small_txn_count": 12},
}


def _configs_dir() -> Path:
    for raw in (
        os.getenv("CONFIGS_DIR", "").strip(),
        "/app/configs",
        str(Path(__file__).resolve().parents[2] / "configs"),
    ):
        if not raw:
            continue
        p = Path(raw)
        if p.is_dir():
            return p
    return Path(__file__).resolve().parents[2] / "configs"


def _rules_json_path() -> Path:
    explicit = os.getenv("RULES_JSON_PATH", "").strip()
    if explicit:
        p = Path(explicit)
        return p if p.is_file() else _configs_dir() / "rules.json"
    return _configs_dir() / "rules.json"


def _rules_yaml_path() -> Path:
    explicit = os.getenv("RULES_PATH", "").strip()
    if explicit:
        p = Path(explicit)
        return p if p.is_file() else _configs_dir() / "rules.yaml"
    return _configs_dir() / "rules.yaml"


def load_rules() -> dict:
    rules_json = _rules_json_path()
    rules_yaml = _rules_yaml_path()
    if rules_json.is_file():
        with open(rules_json) as f:
            data = json.load(f)
        merged = DEFAULT_RULES.copy()
        merged.update(data)
        for k, v in data.items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k] = {**DEFAULT_RULES.get(k, {}), **v}
        return merged
    if rules_yaml.is_file():
        with open(rules_yaml) as f:
            return yaml.safe_load(f)
    return DEFAULT_RULES.copy()


def save_rules(rules: dict) -> None:
    rules_json = _rules_json_path()
    rules_yaml = _rules_yaml_path()
    rules_json.parent.mkdir(parents=True, exist_ok=True)
    with open(rules_json, "w") as f:
        json.dump(rules, f, indent=2)
        f.write("\n")
    with open(rules_yaml, "w") as f:
        yaml.dump(rules, f, default_flow_style=False, sort_keys=False)
