"""Alert priority scoring — hybrid rule signal + KYC risk."""

from __future__ import annotations

RULE_BASE_SCORES: dict[str, float] = {
    "geographic": 90,
    "high_value": 85,
    "smurfing": 80,
    "weekly_volume": 70,
    "daily_velocity": 65,
    "monthly_peer_anomaly": 60,
    "dormant_reactivation": 75,
    "mule_inbound": 82,
}

DEFAULT_RULE_BASE = 50.0
KYC_WEIGHT = 0.40
RULE_WEIGHT = 0.60


def rule_signal(rule_name: str, amount_eur: float) -> float:
    base = RULE_BASE_SCORES.get(rule_name, DEFAULT_RULE_BASE)
    bonus = min(float(amount_eur or 0) / 500.0, 10.0)
    return min(100.0, base + bonus)


def alert_priority_score(rule_name: str, amount_eur: float, kyc_risk_score: float) -> float:
    signal = rule_signal(rule_name, amount_eur)
    kyc = float(kyc_risk_score or 0)
    return round(RULE_WEIGHT * signal + KYC_WEIGHT * kyc, 2)
