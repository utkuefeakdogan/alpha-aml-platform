"""Enterprise banking transaction generator with parties, FX, and anomalies."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker

from src.generator.customer_generator import maybe_acquire_customer
from src.generator.customer_loader import (
    CustomerRecord,
    build_fallback_registry,
    fetch_random_active_customer,
    load_customers_from_db,
)
from src.generator.fx import FX_TO_EUR, SUPPORTED_CURRENCIES, to_eur

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    Producer = None  # type: ignore

logger = logging.getLogger(__name__)
fake = Faker(["de_DE", "tr_TR"])

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "transactions.raw")
INTERVAL_MIN_SEC = float(os.getenv("TXN_INTERVAL_MIN_SEC", "10"))
INTERVAL_MAX_SEC = float(os.getenv("TXN_INTERVAL_MAX_SEC", "20"))
CUSTOMER_CACHE_MAX = int(os.getenv("CUSTOMER_CACHE_MAX", "100"))
NORMAL_MAX_AMOUNT_EUR = float(os.getenv("NORMAL_MAX_AMOUNT_EUR", "8500"))

TXN_CATEGORIES: dict[str, list[str]] = {
    "MI": ["INTERNAL_TRANSFER", "ACCOUNT_MAINTENANCE"],
    "Cash": ["ATM_DEPOSIT", "ATM_WITHDRAWAL", "CASH_TELLER"],
    "Wire": ["FAST", "SWIFT", "SEPA"],
    "BackOffice": ["FX", "ADJUSTMENT", "SETTLEMENT"],
}

ALLOWED_COUNTRIES = ["DE", "FR", "NL", "AT", "BE", "IT", "ES", "PL", "CH", "TR"]
DEFAULT_HIGH_RISK = ["RU", "KP"]

DESCRIPTIONS: dict[str, list[str]] = {
    "INTERNAL_TRANSFER": ["Hesaplar arası transfer", "Internal account transfer", "Virman"],
    "FAST": ["FAST anlık ödeme", "Instant payment", "Hızlı havale"],
    "SWIFT": ["Uluslararası SWIFT havale", "Cross-border wire", "Yurtdışı transfer"],
    "SEPA": ["SEPA ödeme", "SEPA credit transfer"],
    "ATM_DEPOSIT": ["ATM nakit yatırma", "Cash deposit at ATM"],
    "ATM_WITHDRAWAL": ["ATM nakit çekim", "Cash withdrawal"],
    "CASH_TELLER": ["Gişe nakit işlem", "Teller cash transaction"],
    "FX": ["Döviz alım/satım", "FX conversion"],
    "ADJUSTMENT": ["Muhasebe düzeltme", "Ledger adjustment"],
    "SETTLEMENT": ["Mutabakat ödemesi", "Settlement payment"],
    "ACCOUNT_MAINTENANCE": ["Hesap bakım ücreti", "Account maintenance fee"],
}


@dataclass
class ExternalParty:
    name: str
    country: str


def _trim_cache(cache: dict[str, CustomerRecord]) -> None:
    if len(cache) > CUSTOMER_CACHE_MAX:
        for key in list(cache.keys())[: len(cache) - CUSTOMER_CACHE_MAX]:
            del cache[key]


def _external_party(
    external_registry: dict[str, ExternalParty],
    country: str | None = None,
) -> tuple[str, str, str]:
    """Return stable (name, identity_no, country) for external parties."""
    if external_registry and random.random() < 0.55:
        identity, party = random.choice(list(external_registry.items()))
        return party.name, identity, party.country
    identity = f"{random.randint(10000000000, 99999999999)}"
    resolved_country = country or random.choice(ALLOWED_COUNTRIES)
    party = ExternalParty(name=fake.name(), country=resolved_country)
    external_registry[identity] = party
    return party.name, identity, party.country


@dataclass
class SmurfingState:
    active: bool = False
    sender: CustomerRecord | None = None
    receivers_sent: set[str] = field(default_factory=set)
    txn_count: int = 0
    target_count: int = 15
    max_amount: float = 500.0

    def maybe_start(self, registry: dict[str, CustomerRecord]) -> None:
        if self.active or random.random() > 0.12:
            return
        self.active = True
        self.sender = random.choice(list(registry.values()))
        self.receivers_sent = set()
        self.txn_count = 0
        self.target_count = random.randint(12, 18)
        logger.info("Smurfing started for %s", self.sender.customer_id)

    def next_txn(self, registry: dict[str, CustomerRecord], external_registry: dict[str, ExternalParty]) -> dict | None:
        if not self.active or not self.sender:
            return None
        receiver_name, receiver_identity, _ = _external_party(external_registry)
        self.txn_count += 1
        txn = _build_outbound(
            registry,
            self.sender,
            receiver_customer_no=None,
            receiver_name=receiver_name,
            receiver_identity_no=receiver_identity,
            external_registry=external_registry,
            txn_category="Wire",
            txn_type="FAST",
            amount=round(random.uniform(50, self.max_amount), 2),
            currency=random.choice(["EUR", "TRY", "USD"]),
            is_fraud=True,
            fraud_type="smurfing",
        )
        if self.txn_count >= self.target_count:
            self.active = False
        return txn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_currency() -> str:
    weights = [0.45, 0.25, 0.20, 0.07, 0.03]
    return random.choices(SUPPORTED_CURRENCIES, weights=weights)[0]


def _txn_description(txn_type: str, sender_name: str, receiver_name: str) -> str:
    templates = DESCRIPTIONS.get(txn_type, ["Ödeme", "Payment"])
    base = random.choice(templates)
    if random.random() < 0.4:
        return f"{base} — {sender_name} → {receiver_name}"
    if random.random() < 0.5:
        return f"{base} ref {random.randint(100000, 999999)}"
    return base


def _build_outbound(
    registry: dict[str, CustomerRecord],
    sender: CustomerRecord,
    receiver_customer_no: str | None,
    receiver_name: str,
    receiver_identity_no: str,
    external_registry: dict[str, ExternalParty] | None = None,
    txn_category: str | None = None,
    txn_type: str | None = None,
    amount: float | None = None,
    currency: str | None = None,
    country_code: str | None = None,
    is_fraud: bool = False,
    fraud_type: str | None = None,
) -> dict:
    category = txn_category or random.choice(list(TXN_CATEGORIES.keys()))
    ttype = txn_type or random.choice(TXN_CATEGORIES[category])
    cur = currency or _pick_currency()
    amt = round(amount if amount is not None else random.uniform(50, 8000), 2)
    recv_branch = None
    recv_country = None
    if receiver_customer_no and receiver_customer_no in registry:
        recv = registry[receiver_customer_no]
        recv_branch = recv.branch_id
        recv_country = recv.country
    elif external_registry and receiver_identity_no in external_registry:
        recv_country = external_registry[receiver_identity_no].country
    elif receiver_identity_no:
        recv_country = country_code
    return _txn_payload(
        sender_customer_no=sender.customer_id,
        sender_name=sender.name,
        sender_identity_no=sender.identity_no,
        sender_branch=sender.branch_id,
        sender_country=sender.country,
        receiver_customer_no=receiver_customer_no,
        receiver_name=receiver_name,
        receiver_identity_no=receiver_identity_no,
        receiver_branch=recv_branch,
        receiver_country=recv_country,
        txn_category=category,
        txn_type=ttype,
        branch_id=sender.branch_id,
        amount=amt,
        currency=cur,
        country_code=country_code or sender.country,
        is_customer_sender=True,
        is_customer_receiver=receiver_customer_no is not None,
        is_fraud=is_fraud,
        fraud_type=fraud_type,
    )


def _cap_normal_amount(amount: float, currency: str) -> float:
    eur = to_eur(amount, currency)
    if eur <= NORMAL_MAX_AMOUNT_EUR:
        return amount
    rate = FX_TO_EUR.get(currency.upper(), 1.0) or 1.0
    return round(NORMAL_MAX_AMOUNT_EUR / rate, 2)


def _build_inbound_external(
    registry: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
) -> dict:
    """Inbound wire from another bank — sender customer no empty, name/identity known."""
    receiver = random.choice(list(registry.values()))
    ext_name, ext_identity, ext_country = _external_party(external_registry)
    cur = random.choice(SUPPORTED_CURRENCIES)
    amt = _cap_normal_amount(round(random.uniform(500, 12000), 2), cur)
    return _txn_payload(
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
        txn_type="SWIFT",
        branch_id=receiver.branch_id,
        amount=amt,
        currency=cur,
        country_code=ext_country,
        is_customer_sender=False,
        is_customer_receiver=True,
        is_fraud=False,
    )


def _build_internal_transfer(registry: dict[str, CustomerRecord]) -> dict:
    sender, receiver = random.sample(list(registry.values()), 2)
    cur = _pick_currency()
    amt = _cap_normal_amount(round(random.uniform(100, 12000), 2), cur)
    return _txn_payload(
        sender_customer_no=sender.customer_id,
        sender_name=sender.name,
        sender_identity_no=sender.identity_no,
        sender_branch=sender.branch_id,
        sender_country=sender.country,
        receiver_customer_no=receiver.customer_id,
        receiver_name=receiver.name,
        receiver_identity_no=receiver.identity_no,
        receiver_branch=receiver.branch_id,
        receiver_country=receiver.country,
        txn_category="MI",
        txn_type="INTERNAL_TRANSFER",
        branch_id=sender.branch_id,
        amount=amt,
        currency=cur,
        country_code=sender.country,
        is_customer_sender=True,
        is_customer_receiver=True,
    )


def _txn_payload(
    sender_customer_no: str | None,
    sender_name: str,
    sender_identity_no: str,
    sender_branch: str | None,
    sender_country: str | None,
    receiver_customer_no: str | None,
    receiver_name: str,
    receiver_identity_no: str,
    receiver_branch: str | None,
    receiver_country: str | None,
    txn_category: str,
    txn_type: str,
    branch_id: str,
    amount: float,
    currency: str,
    country_code: str,
    is_customer_sender: bool,
    is_customer_receiver: bool,
    is_fraud: bool = False,
    fraud_type: str | None = None,
) -> dict:
    amount_eur = to_eur(amount, currency)
    risk_country = sender_country or receiver_country or country_code
    txn = {
        "txn_id": str(uuid.uuid4()),
        "txn_category": txn_category,
        "txn_type": txn_type,
        "sender_id": sender_customer_no,
        "receiver_id": receiver_customer_no or f"EXT-{uuid.uuid4().hex[:10]}",
        "sender_customer_no": sender_customer_no,
        "receiver_customer_no": receiver_customer_no,
        "sender_name": sender_name,
        "receiver_name": receiver_name,
        "sender_identity_no": sender_identity_no,
        "receiver_identity_no": receiver_identity_no,
        "sender_branch": sender_branch,
        "receiver_branch": receiver_branch,
        "sender_country": sender_country,
        "receiver_country": receiver_country,
        "txn_description": _txn_description(txn_type, sender_name, receiver_name),
        "branch_id": branch_id,
        "amount": amount,
        "currency": currency,
        "amount_eur": amount_eur,
        "country_code": risk_country,
        "is_customer_sender": is_customer_sender,
        "is_customer_receiver": is_customer_receiver,
        "ts": _iso_now(),
        "is_synthetic_fraud": is_fraud,
    }
    if fraud_type:
        txn["fraud_type"] = fraud_type
    return txn


def _ensure_cache_entry(cache: dict[str, CustomerRecord]) -> None:
    if cache:
        return
    try:
        c = fetch_random_active_customer()
        if c:
            cache[c.customer_id] = c
    except Exception as exc:
        logger.warning("Could not sample customer: %s", exc)


def _normal_txn(
    cache: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
) -> dict:
    _ensure_cache_entry(cache)
    if not cache:
        fallback = build_fallback_registry(50)
        cache.update(fallback)
    roll = random.random()
    if roll < 0.12:
        return _build_inbound_external(cache, external_registry)
    if roll < 0.35:
        return _build_internal_transfer(cache)
    if random.random() < 0.4:
        try:
            c = fetch_random_active_customer()
            if c:
                cache[c.customer_id] = c
        except Exception:
            pass
    sender = random.choice(list(cache.values()))
    receiver = random.choice(list(cache.values()))
    if receiver.customer_id == sender.customer_id:
        receiver_name, receiver_identity, _ = _external_party(external_registry)
        receiver_no = None
    else:
        receiver_name, receiver_identity = receiver.name, receiver.identity_no
        receiver_no = receiver.customer_id
    txn = _build_outbound(
        cache, sender, receiver_no, receiver_name, receiver_identity,
        external_registry=external_registry,
    )
    txn["amount"] = _cap_normal_amount(txn["amount"], txn["currency"])
    txn["amount_eur"] = to_eur(txn["amount"], txn["currency"])
    return txn


def _geographic_txn(
    registry: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    high_risk: list[str],
) -> dict:
    receiver = random.choice(list(registry.values()))
    # Force the high-risk country so the geographic rule always matches — the
    # external-party helper may return a cached party from a non-risk country.
    risk_country = random.choice(high_risk)
    ext_name, ext_identity, _ = _external_party(external_registry, country=risk_country)
    cur = random.choice(["USD", "EUR"])
    amt = round(random.uniform(5000, 80000), 2)
    return _txn_payload(
        sender_customer_no=None,
        sender_name=ext_name,
        sender_identity_no=ext_identity,
        sender_branch=None,
        sender_country=risk_country,
        receiver_customer_no=receiver.customer_id,
        receiver_name=receiver.name,
        receiver_identity_no=receiver.identity_no,
        receiver_branch=receiver.branch_id,
        receiver_country=receiver.country,
        txn_category="Wire",
        txn_type="SWIFT",
        branch_id=receiver.branch_id,
        amount=amt,
        currency=cur,
        country_code=risk_country,
        is_customer_sender=False,
        is_customer_receiver=True,
        is_fraud=True,
        fraud_type="geographic",
    )


def _high_value_txn(registry: dict[str, CustomerRecord], external_registry: dict[str, ExternalParty], threshold_eur: float) -> dict:
    sender = random.choice(list(registry.values()))
    ext_name, ext_identity, _ = _external_party(external_registry)
    cur = random.choice(["EUR", "USD", "GBP"])
    amt = (threshold_eur + random.uniform(1000, 20000)) / FX_TO_EUR[cur]
    return _build_outbound(
        registry,
        sender,
        receiver_customer_no=None,
        receiver_name=ext_name,
        receiver_identity_no=ext_identity,
        external_registry=external_registry,
        txn_category="Wire",
        txn_type="SWIFT",
        amount=round(amt, 2),
        currency=cur,
        is_fraud=True,
        fraud_type="high_value",
    )


def _velocity_burst(
    registry: dict[str, CustomerRecord],
    external_registry: dict[str, ExternalParty],
    size: int = 6,
) -> list[dict]:
    sender = random.choice(list(registry.values()))
    txns = []
    for _ in range(size):
        rname, rid, _ = _external_party(external_registry)
        txns.append(
            _build_outbound(
                registry,
                sender,
                None,
                rname,
                rid,
                external_registry=external_registry,
                txn_category="Wire",
                txn_type="FAST",
                amount=round(random.uniform(30, 400), 2),
                currency=random.choice(["EUR", "TRY"]),
                is_fraud=True,
                fraud_type="velocity",
            )
        )
    return txns


def ensure_topic(producer_config: dict) -> None:
    admin = AdminClient({"bootstrap.servers": producer_config["bootstrap.servers"]})
    if KAFKA_TOPIC in admin.list_topics(timeout=10).topics:
        return
    admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)])


def create_producer(retries: int = 5) -> Producer:
    if Producer is None:
        raise RuntimeError("confluent-kafka not installed")
    config = {"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "transaction-gen", "acks": "1"}
    for attempt in range(1, retries + 1):
        try:
            producer = Producer(config)
            ensure_topic(config)
            producer.list_topics(timeout=10)
            return producer
        except Exception as exc:
            logger.warning("Kafka connect %d/%d: %s", attempt, retries, exc)
            time.sleep(min(2 ** attempt, 30))
    raise ConnectionError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")


def publish(producer: Producer, txn: dict) -> None:
    producer.produce(KAFKA_TOPIC, json.dumps(txn).encode("utf-8"))
    producer.flush(timeout=5)


def run_loop(producer: Producer | None = None) -> None:
    rules = {}
    path = Path(os.getenv("RULES_JSON_PATH", "/app/configs/rules.json"))
    if path.exists():
        with open(path) as f:
            rules = json.load(f)
    customer_cache: dict[str, CustomerRecord] = {}
    try:
        customer_cache.update(load_customers_from_db(limit=50))
    except Exception as exc:
        logger.warning("Using fallback customer sample: %s", exc)
        customer_cache.update(build_fallback_registry(50))

    from src.generator.scenario_injectors import run_scenario_inject
    from src.generator.scenario_scheduler import ScenarioScheduler

    scheduler = ScenarioScheduler()
    external_registry: dict[str, ExternalParty] = {}
    logger.info(
        "Generator started — scenario scheduler (%d rules), DB random sampling",
        scheduler.enabled_count(),
    )
    own = producer is None
    if own:
        producer = create_producer()

    try:
        while True:
            if scheduler.due():
                scenario = scheduler.pick_scenario()
                if scenario:
                    txns = run_scenario_inject(scenario, customer_cache, external_registry, rules)
                    for txn in txns:
                        publish(producer, txn)
                    logger.info(
                        "Scenario %s (%s): %d txns",
                        scenario.get("id"),
                        scenario.get("rule_name"),
                        len(txns),
                    )
            else:
                txn = _normal_txn(customer_cache, external_registry)
                publish(producer, txn)
                logger.info(
                    "Txn %s %.2f %s (%.2f EUR) %s→%s",
                    txn["txn_id"][:8],
                    txn["amount"],
                    txn["currency"],
                    txn["amount_eur"],
                    txn["sender_customer_no"] or "EXT",
                    txn["receiver_customer_no"] or "EXT",
                )
            maybe_acquire_customer(customer_cache, customer_cache)
            _trim_cache(customer_cache)
            time.sleep(random.uniform(INTERVAL_MIN_SEC, INTERVAL_MAX_SEC))
    except KeyboardInterrupt:
        logger.info("Generator stopped")
    finally:
        if own and producer:
            producer.flush(timeout=5)


def hash_account(account_id: str) -> str:
    return hashlib.sha256(account_id.encode()).hexdigest()[:16]
