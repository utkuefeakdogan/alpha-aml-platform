"""Scenario-specific transaction injectors (catalog-driven)."""

from __future__ import annotations

import random
from typing import Callable

from src.generator.customer_loader import (
    CustomerRecord,
    fetch_random_active_customer,
    fetch_random_dormant_customer,
)
from src.generator.transaction_generator import (
    ExternalParty,
    _build_outbound,
    _geographic_txn,
    _high_value_txn,
    _txn_payload,
    _external_party,
)

Handler = Callable[..., list[dict]]

# Note: monthly_peer_anomaly is intentionally NOT injected synthetically. The
# rule stays active in the streaming engine and is detected organically from
# genuinely high-frequency accounts (monthly txn_count > baseline x multiplier).


def _active_pool(cache: dict[str, CustomerRecord]) -> dict[str, CustomerRecord]:
    if not cache:
        c = fetch_random_active_customer()
        if c:
            cache[c.customer_id] = c
    return cache


def _one_active(cache: dict[str, CustomerRecord]) -> CustomerRecord:
    pool = _active_pool(cache)
    if not pool:
        raise RuntimeError("No active customer available")
    if random.random() < 0.35:
        c = fetch_random_active_customer()
        if c:
            cache[c.customer_id] = c
            return c
    return random.choice(list(pool.values()))


def inject_geographic(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
) -> list[dict]:
    high_risk = rules.get("geographic", {}).get("high_risk_countries", ["RU", "KP"])
    pool = _active_pool(cache)
    return [_geographic_txn(pool, external_registry, high_risk)]


def inject_high_value(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
) -> list[dict]:
    threshold = float(rules.get("high_value", {}).get("threshold_eur", 10000))
    pool = _active_pool(cache)
    return [_high_value_txn(pool, external_registry, threshold)]


def inject_smurfing(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
    max_txns: int = 9,
) -> list[dict]:
    sender = _one_active(cache)
    cache[sender.customer_id] = sender
    smurf = rules.get("smurfing", {})
    max_amt = float(smurf.get("weekly_small_txn_threshold_eur", 500)) * 0.9
    txns = []
    for _ in range(max_txns):
        rname, rid, _ = _external_party(external_registry)
        txns.append(
            _build_outbound(
                cache,
                sender,
                None,
                rname,
                rid,
                external_registry=external_registry,
                txn_category="Wire",
                txn_type="FAST",
                amount=round(random.uniform(50, max_amt), 2),
                currency=random.choice(["EUR", "TRY"]),
                is_fraud=True,
                fraud_type="smurfing",
            )
        )
    return txns


def inject_daily_velocity(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
    max_txns: int = 6,
) -> list[dict]:
    sender = _one_active(cache)
    cache[sender.customer_id] = sender
    # Keep amounts under the rule's amount ceiling: many small rapid transfers.
    amount_cap = float(rules.get("multi_window", {}).get("daily_velocity_max_amount_eur", 1000))
    max_amt = max(50.0, amount_cap * 0.9)
    txns = []
    for _ in range(max_txns):
        rname, rid, _ = _external_party(external_registry)
        txns.append(
            _build_outbound(
                cache,
                sender,
                None,
                rname,
                rid,
                external_registry=external_registry,
                txn_category="Wire",
                txn_type="FAST",
                amount=round(random.uniform(30, max_amt), 2),
                currency=random.choice(["EUR", "TRY"]),
                is_fraud=True,
                fraud_type="velocity",
            )
        )
    return txns


def inject_weekly_volume(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
    max_txns: int = 1,
) -> list[dict]:
    mw = rules.get("multi_window", {})
    weekly_max = float(mw.get("weekly_volume_max_eur", 10000))
    sender = _one_active(cache)
    cache[sender.customer_id] = sender
    rname, rid, _ = _external_party(external_registry)
    return [
        _build_outbound(
            cache,
            sender,
            None,
            rname,
            rid,
            external_registry=external_registry,
            txn_category="Wire",
            txn_type="SWIFT",
            amount=round(weekly_max + random.uniform(500, 2500), 2),
            currency="EUR",
            is_fraud=True,
            fraud_type="weekly_volume",
        )
    ]


def inject_dormant_reactivation(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
    max_txns: int = 2,
) -> list[dict]:
    dormant = fetch_random_dormant_customer()
    if not dormant:
        return inject_high_value(cache, external_registry, rules)
    cache[dormant.customer_id] = dormant
    min_eur = float(rules.get("dormant_reactivation", {}).get("min_amount_eur", 3000))
    txns = []
    for _ in range(max_txns):
        rname, rid, _ = _external_party(external_registry)
        txns.append(
            _build_outbound(
                cache,
                dormant,
                None,
                rname,
                rid,
                external_registry=external_registry,
                txn_category="Wire",
                txn_type="SWIFT",
                amount=round(min_eur + random.uniform(500, 5000), 2),
                currency="EUR",
                is_fraud=True,
                fraud_type="dormant_reactivation",
            )
        )
    return txns


def inject_mule_inbound(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
    max_txns: int = 7,
) -> list[dict]:
    receiver = _one_active(cache)
    cache[receiver.customer_id] = receiver
    mule = rules.get("mule_inbound", {})
    min_amt = float(mule.get("min_total_amount_eur", 500)) / max(max_txns, 1)
    txns = []
    for _ in range(max_txns):
        ext_name, ext_identity, ext_country = _external_party(external_registry)
        cur = random.choice(["EUR", "USD"])
        amt = round(min_amt + random.uniform(100, 2000), 2)
        txns.append(
            _txn_payload(
                sender_customer_no=None,
                sender_name=ext_name,
                sender_identity_no=ext_identity,
                sender_branch=None,
                sender_country=ext_country,
                receiver_customer_no=receiver.customer_id,
                receiver_name=receiver.name,
                receiver_identity_no=receiver.identity_no,
                receiver_branch=receiver.branch_id,
                receiver_country=receiver.country,
                txn_category="Wire",
                txn_type="FAST",
                branch_id=receiver.branch_id,
                amount=amt,
                currency=cur,
                country_code=ext_country,
                is_customer_sender=False,
                is_customer_receiver=True,
                is_fraud=True,
                fraud_type="mule_inbound",
            )
        )
    return txns


HANDLERS: dict[str, Handler] = {
    "inject_geographic": inject_geographic,
    "inject_high_value": inject_high_value,
    "inject_smurfing": inject_smurfing,
    "inject_daily_velocity": inject_daily_velocity,
    "inject_weekly_volume": inject_weekly_volume,
    "inject_dormant_reactivation": inject_dormant_reactivation,
    "inject_mule_inbound": inject_mule_inbound,
}


def run_scenario_inject(
    scenario: dict,
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    rules: dict,
) -> list[dict]:
    handler_name = scenario.get("generator_handler", "")
    handler = HANDLERS.get(handler_name)
    if not handler:
        raise ValueError(f"Unknown scenario handler: {handler_name}")
    max_txns = int(scenario.get("max_txns_per_inject", 1))
    if handler_name in ("inject_geographic", "inject_high_value"):
        return handler(cache, external_registry, rules)
    return handler(cache, external_registry, rules, max_txns=max_txns)
