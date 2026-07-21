# Alpha AML — Project Evaluation

> A short, final assessment of the platform: what I set out to build, the decisions behind it, and where it goes next.
> For the deep technical walkthrough see [`efe_system.md`](efe_system.md); for the overview and quickstart see [`README.md`](README.md).
> **Live:** https://utku-efe-aml.duckdns.org · **Environment:** single Oracle Cloud free-tier VM (6 GB RAM).

---

## 1. Purpose

I set out to build a **real-time anti–money-laundering / fraud-detection backbone** for a simulated European bank — not a notebook, but a system an interviewer can click through live. The goals were:

- **End-to-end ownership** across data engineering, analytics and ML: streaming ingestion, stateful processing, a modeled warehouse, orchestration, serving and ops.
- Grounding every component in **real AML domain logic** (KYC/PEP risk, SAR filing, money-laundering typologies) rather than generic CRUD.
- Running **reliably and cost-aware 24/7 on a single 6 GB VM**, behind HTTPS.

All data is synthetic (Faker + scenario injectors); no real customer or payment data is used.

---

## 2. Architecture at a glance

```
transaction-gen → Kafka (transactions.raw) → Spark Structured Streaming (30s)
   → PostgreSQL (aml.*) → dbt (bronze/silver/gold) + Streamlit + GenAI SAR worker
Airflow orchestrates retention / audit / dbt / SAR / ML scoring / disk-guard.
Caddy reverse proxy terminates HTTPS in front of the dashboard.
```

| Layer | Tech | Why |
|---|---|---|
| Ingestion | Python, Faker, confluent-kafka | reproducible synthetic typologies |
| Bus | Apache Kafka | replayable, decoupled buffer |
| Processing | PySpark Structured Streaming | windowed, stateful rule engine in one engine |
| Storage | PostgreSQL 16 | ACID, SQL audit, one store for ops + warehouse + Airflow meta |
| Transform | dbt | tested medallion lineage (bronze → silver → gold) |
| ML | scikit-learn (Isolation Forest + Gradient Boosting) | always-on, descriptive risk layer |
| Serving | Streamlit (11 pages, EN/DE/TR) | human-in-the-loop compliance UI |
| GenAI | OpenAI (mock fallback) | SAR drafting |
| Orchestration | Apache Airflow | retention, audits, dbt, SAR, ML, disk-guard |
| Edge | Caddy + Let's Encrypt | automatic HTTPS |
| Packaging | Docker Compose (profiles) | 6 GB RAM budget |

---

## 3. Key design decisions & trade-offs

- **Kafka as the bus** — replayability and producer/consumer decoupling justify its ~1.1 GB ZK+broker cost; accepted as an industry-standard trade-off on a small VM.
- **Spark Structured Streaming** for windowed stateful rules in a single engine (30 s micro-batch). On `local[2]` / 512 m the driver is the main constraint — it keeps up but is the first thing to scale.
- **One PostgreSQL** for operational tables, the dbt warehouse and Airflow metadata — simple, ACID, and the model compliance teams already understand.
- **Thresholds as versioned JSON** (`configs/`) shared by the engine and the dashboard — a single source of truth — plus **alert budgeting** (≤100/day, ≤12/rule) to model real analyst fatigue.
- **ML as a descriptive "9th scenario"** that raises no alerts — it augments the rules and is benchmarked against them, with metrics reported honestly given scarce, budget-capped labels.
- **Compose profiles** (`core` / `app` / `ops` / `edge`) keep the stack inside 6 GB; the rule of thumb is *don't run `ops` (Airflow) under heavy `app` load*.
- **Security by default** — only Caddy's 80/443 are public; Postgres and Kafka have no host ports; the SQL explorer runs as a SELECT-only role; SAR data is PII-hashed.

---

## 4. What it demonstrates

- **Data Engineering** — event streaming, stateful stream processing with windowed aggregates and idempotent upserts, a tested medallion model in dbt, workflow orchestration, containerization, and pragmatic resource/cost optimization on constrained infrastructure.
- **Data Analysis** — a tested SQL warehouse and an interactive analytics/compliance dashboard (alert trends, risk-band & PEP incidence, segmentation, cross-border corridors, customer 360) framed around real AML metrics.
- **Data Science** — an always-on ML risk layer (Isolation Forest anomaly + Gradient Boosting triage) with leakage-safe features, stratified-CV evaluation (ROC-AUC, PR-AUC), and feature-importance / rules-vs-ML overlap surfaced in the dashboard.

---

## 5. How it grows

- **Scale the stream** — broadcast/cache the customer table and move the per-batch behavior overwrite out of the stream to clear Spark backpressure; partition the Kafka topic and move from `local[2]` toward a real executor/cluster.
- **Tighten lifecycle** — prune `account_window_metrics` by `window_start` of closed windows + `VACUUM`, and bound `pipeline_metrics`.
- **Engineering rigor** — GitHub Actions CI already runs **ruff** + **`dbt parse`** on every push/PR; next steps are a small `pytest` suite and (optionally) `dbt test` against a CI Postgres service.
- **Cloud analytics bridge** — scheduled Gold sync to BigQuery (Postgres → Parquet → GCS → BQ), then thin Terraform for dataset/IAM (planned; not claimed until live).
- **Richer ML** — accumulate alert labels over time, add a feature store and drift monitoring.
- **Hardening** — move secrets to Vault / Oracle Secrets and rotate the DB password.

---

## 6. Known limitations

- Single-node Spark (512 m, `local[2]`) keeps up but is the first bottleneck under load.
- Synthetic data, and labels are scarce by design (alerts are budget-capped), so supervised metrics are modest and reported with their sample sizes.
- Single VM — no high availability; HTTPS aside, this is a demo deployment, not production infrastructure.

---

## 7. Operations (quick reference)

```bash
make up      # core + app (Kafka, Postgres, generator, Spark, dashboard)
make ops     # + Airflow (run only when needed, watch RAM)
make edge    # + Caddy/HTTPS reverse proxy (live demo)
make down    # stop everything (data persists in volumes)
```

| Endpoint | Access |
|---|---|
| Public (live) | https://utku-efe-aml.duckdns.org — Caddy + Let's Encrypt |
| Dashboard (local) | http://localhost:8501 (bound to 127.0.0.1) |
| Airflow | http://localhost:8080 (admin / admin, after `make ops`) |
| Postgres / Kafka | internal only — no host port (`docker exec … psql`) |

**RAM budget:** ZK + Kafka + Postgres ≈ 2.2 GB (`core`) · generator + SAR + Streamlit ≈ 0.9 GB and Spark ≈ 1.5 GB (`app`) · Airflow ≈ 1.2 GB (`ops`). Stopping the stack when idle frees almost all of it; data survives in the `pgdata` volume.

---

*Last updated after the live HTTPS deployment. `README.md` is the project overview; `efe_system.md` is the deep technical walkthrough; this file is the evaluation/decision context.*
