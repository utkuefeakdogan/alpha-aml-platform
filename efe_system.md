# Alpha AML — System Walkthrough & Operations Notes

This is my own deep walkthrough of the platform I built: every service, the schema and migrations, the DAGs, dbt, the dashboard, and the runtime behavior I've verified against the live database. I keep it next to [`EVALUATION.md`](EVALUATION.md) (architecture/operations rationale) so I — and anyone reviewing the project — can understand exactly how the system works and why I made each decision.

A framing note before the 12 sections: **the code in the repo is the post-incident, largely-remediated state.** Early on the platform hit a disk/metrics-bloat incident, and I rebuilt the affected paths; the live DB confirms recovery — `account_window_metrics` is back down to ~6,000 rows, disk is healthy, and all containers are healthy. So for each incident I explain the original root cause *and* what I've already fixed in the current files versus what's still worth tightening. The project is on GitHub at `github.com/utkuefeakdogan/alpha-aml-platform`.

> **Update note (current state):** since this walkthrough was first written the project has evolved further — the legacy `aml.raw_transactions` table and its `cleanup_raw_data`/`stg_raw_transactions` lineage were retired (migration `021`; Kafka `transactions.raw` is now the bronze layer); an always-on **ML risk layer** was added (`src/ml/`, `aml.ml_customer_scores` + `aml.ml_model_runs`, `ml_score` DAG every 6h); a **central event log** (`aml.event_log`) plus new dashboard pages (Scenarios, Risk Models, Analytics, System Health, Logs, SQL Explorer) were added; Airflow runs **SequentialExecutor**; Postgres mem limit is **1024m** and Airflow **1200m**; and Postgres/Kafka host ports were closed (internal-only). The sections below note these where relevant.

---

# 1. System Architecture

**What it is:** "Alpha AML Pipeline" — a real-time **Anti-Money-Laundering / fraud-detection platform** for a simulated European bank. It generates synthetic banking transactions, streams them through Kafka, applies windowed AML rules in Spark, persists everything in PostgreSQL, transforms bronze→silver→gold with dbt, surfaces alerts in a Streamlit compliance dashboard, and auto-drafts **SAR** (Suspicious Activity Reports) with a GenAI worker. It's explicitly built as a senior/lead data-engineering interview showcase tuned for a 6 GB Oracle Cloud free-tier VM (see `README.md`, `EVALUATION.md`).

**End-to-end:** `transaction-gen` → Kafka topic `transactions.raw` → `spark-job` (30s micro-batches) → Postgres `aml.*` → dbt (gold models) / Streamlit / `sar-worker`, with Airflow orchestrating retention, audits, dbt runs, SAR triggers, ML scoring, and disk guarding.

**Services** (from `docker-compose.yml`, grouped by profile):

| Service | Profile | Mem limit | Role |
|---|---|---|---|
| `zookeeper` | core | 384m | Kafka coordination |
| `kafka` | core | 768m | Event bus |
| `postgres` | core, ops | 1024m | System of record (`aml` schema) |
| `transaction-gen` | app | 256m | Synthetic transaction producer |
| `spark-job` | app | 1536m | Structured Streaming rule engine |
| `streamlit` | app | 384m | Compliance dashboard (port 8501, only host-exposed service) |
| `sar-worker` | app | 256m | GenAI SAR generator (300s loop) |
| `airflow-init` | ops | 512m | DB migration/init (SequentialExecutor) |
| `airflow` | ops | 1200m | SequentialExecutor scheduler + webserver + DAGs |

**ASCII data-flow diagram:**

```
                         configs/rules.json + rules.yaml + scenario_catalog.json
                                         │ (thresholds, scenarios)
                                         ▼
┌───────────────────┐   JSON    ┌──────────────────┐   subscribe   ┌──────────────────────────┐
│  transaction-gen  │──────────▶│  Kafka topic     │──────────────▶│   spark-job (PySpark      │
│ (Faker + scenario │  produce  │ transactions.raw │   latest      │   Structured Streaming)   │
│  scheduler)       │           │ (1 partition)    │               │   foreachBatch, 30s       │
└─────────┬─────────┘           └──────────────────┘               └────────────┬─────────────┘
          │ samples customers                                                    │ JDBC append/upsert
          │ (RANDOM())                                                           ▼
          │                                              ┌───────────────────────────────────────┐
          └─────────────────────────────────────────────▶          PostgreSQL  (aml schema)      │
                                read flag counts          │  transactions / flagged_transactions  │
                                                          │  account_window_metrics (silver)      │
                                                          │  customer_behavior_30d / customers     │
                                                          │  customer_addresses / sar_reports ...  │
                                                          └───┬───────────────┬──────────────┬────┘
                                                              │ read          │ read         │ read/write
                                                     ┌────────▼──────┐  ┌─────▼──────┐  ┌────▼─────────┐
                                                     │  dbt (gold)   │  │ Streamlit  │  │ sar-worker   │
                                                     │ staging→gold  │  │ dashboard  │  │ (SAR drafts) │
                                                     └───────────────┘  └────────────┘  └──────────────┘
                                                              ▲              
        ┌─────────────────────────────────────────────────  │  ──────────────────────────────────┐
        │                         Airflow (ops profile) DAGs                                        │
        │  cleanup_dag (retention)   dbt_transform (gold)   ml_score (ML scoring, every 6h)          │
        │  disk_guard_dag (disk+truncate)   aml_pipeline_audit (metrics)   trigger_sar (SAR)         │
        └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# 2. Data Flow

**Origin — the transaction generator (`src/generator/`):**
- Entry point `main.py` → `transaction_generator.run_loop()` (`src/generator/transaction_generator.py:492`).
- It loads a **sample** of customers from Postgres via `RANDOM()` (`customer_loader.load_customers_from_db`, kept small — `CUSTOMER_CACHE_MAX=100`), so it never holds the 1.23M universe in memory.
- **Cadence:** one transaction every `TXN_INTERVAL_MIN_SEC`–`TXN_INTERVAL_MAX_SEC` = **10–20 seconds** (`.env`), i.e. ~3–6 txns/min, ~5,000–8,000/day. This is the "burst/simulated real-time" low-CPU mode.
- **What it generates:** realistic enterprise payments (`_txn_payload`, line 276) — txn category/type (MI/Cash/Wire/BackOffice), sender/receiver customer numbers, names, identity numbers, branches, countries, multi-currency amounts with FX→EUR conversion (`src/generator/fx.py`), descriptions in TR/DE/EN.
- **Fraud injection:** a `ScenarioScheduler` (`scenario_scheduler.py`) wakes on a random interval (`SCENARIO_WAKE_MIN/MAX_SEC` = 1200–5400s) and injects one of the **7 synthetically-injected** scenarios from `configs/scenario_catalog.json` (geographic, high_value, smurfing, daily_velocity, weekly_volume, dormant_reactivation, mule_inbound) via `scenario_injectors.py`. The 8th rule, `monthly_peer_anomaly`, stays active in the streaming engine but is **detected organically** (its catalog entry is `enabled: false`) rather than injected. It self-limits to `SCENARIO_DAILY_CAP=100` alerts/day and prioritizes rules with no alerts in the last 7 days; scenarios that produce zero transactions are put on an "empty cooldown" so no single unproductive scenario can monopolize the slot. Normal transactions are amount-capped at `NORMAL_MAX_AMOUNT_EUR=8500` so only injected scenarios cross thresholds.

**Raw data movement, step by step:**
1. Generator serializes the txn dict to JSON and `produce()`s to Kafka `transactions.raw` (`publish`, line 487).
2. Spark `readStream` subscribes (`startingOffsets=latest`, `failOnDataLoss=false`), parses JSON against `TXN_SCHEMA`, filters null `txn_id` (`streaming_job.run`, line 320).
3. Every **30 seconds** (`trigger(processingTime="30 seconds")`) `process_batch` runs (line 236): normalize → enrich → write `aml.transactions` → compute window metrics (upsert) → evaluate rules → apply alert budget → score priority → write `aml.flagged_transactions`.
4. dbt (via `dbt_transform` DAG, `@daily`) reads `aml.*` sources and builds gold tables.
5. Streamlit reads `aml.*` continuously; `sar-worker` polls flagged groups every 300s.

**What triggers each stage:** Kafka arrival → Spark's 30s processing-time trigger drives all transformation. Airflow schedules drive retention (`@hourly`), audit (`@hourly`), dbt (`@daily`), SAR (`*/30`), disk guard (`*/15`).

---

# 3. Data Layers (Bronze / Silver / Gold)

The project maps a medallion architecture onto Kafka + Postgres + dbt:

| Layer | What it is | Stored where | Produced by | Owner |
|---|---|---|---|---|
| **Bronze / ingestion** | Immutable raw landing zone | Kafka `transactions.raw` topic (never persisted as a separate table) | `transaction-gen` produces, `spark-job` consumes | Kafka |
| **Silver (landing)** | Every transaction event as enriched/landed | `aml.transactions` | `spark-job` (`_write_jdbc(txn_write, "aml.transactions")`) | Spark / Postgres |
| **Silver (operational)** | Per-customer multi-window aggregates + 30d behavior + flagged alerts | `aml.account_window_metrics`, `aml.customer_behavior_30d`, `aml.flagged_transactions` | `spark-job` (`window_engine.compute_window_metrics`, `customer_enrichment.compute_customer_behavior_30d`, `evaluate_rules`) | Spark |
| **Silver (dbt staging)** | Cleaned views + data tests | dbt `staging` schema view `stg_flagged_transactions` | dbt | dbt |
| **Gold** | Reporting/risk aggregates | dbt `gold` schema tables: `gold_daily_fraud_summary`, `gold_account_risk_score`, `gold_customer_risk_profile` | dbt (`dbt_transform` DAG) | dbt |

- **Bronze** = the immutable Kafka event stream (`transactions.raw`); bounded by Kafka retention, never a separate table.
- **Silver** = `aml.transactions` (landed/enriched stream) plus Spark's stateful aggregations and rule outputs. `ingested_at`/`flagged_at` ≠ event `ts`, preserving an audit trail.
- **Gold** = business-facing summaries: e.g. `gold_account_risk_score.sql` buckets accounts into high/medium/low by flag count; `gold_daily_fraud_summary.sql` aggregates flags per day/rule; `gold_customer_risk_profile.sql` joins transactions+customers+flags for a 30-day risk profile.

Note: the legacy `aml.raw_transactions` table and its `stg_raw_transactions` staging model were **retired** (migration `021`). Live data lands directly in `aml.transactions`, so the bronze→silver lineage now flows Kafka → `aml.transactions` with no empty intermediate table.

---

# 4. PostgreSQL Schema (`aml`)

Defined in `docker/init.sql` + numbered migrations. Live tables and approximate current sizes (from a live query):

| Table | Purpose | Written by | Read by | Growth |
|---|---|---|---|---|
| `customers` | KYC master / Customer 360 (risk_score, segment, PEP, status, country) | migrations (seed 250; `011` scales to **1,233,000**) | Spark enrichment, generator sampling, dashboard, dbt | Static (235 MB) |
| `customer_addresses` | 1–5 addresses per customer (migration `013`) | migrations | Spark enrichment, dashboard | Static (~3.7M rows, 445 MB) |
| `transactions` | **Silver** landing — enriched enterprise transactions | Spark `_write_jdbc(...,"aml.transactions")` | metrics/behavior history reads, dashboard, dbt | ~5–8k/day (10 MB now) |
| `account_window_metrics` | **Silver** per-customer aggregates × 4 windows | Spark `_write_metrics_upsert` | rule engine, dashboard, disk guard | **bounded now (~6k)**; was 121M |
| `customer_behavior_30d` | 30-day rolling profile per customer | Spark `.mode("overwrite")` each batch | dashboard | bounded (~6.6k) |
| `flagged_transactions` | AML alerts (rule, detail, window, priority) | Spark `_write_jdbc(...,"aml.flagged_transactions")` | dashboard, SAR, dbt, audit | slow (capped ~100/day) |
| `alert_dispositions` | Analyst decisions (FP / SAR filed) | Streamlit `save_alert_disposition` | dashboard | slow (human) |
| `sar_reports` | Generated SAR drafts | `sar-worker`, dashboard | dashboard | slow |
| `pipeline_metrics` | Hourly audit snapshots (migration `012`) | `aml_pipeline_audit` DAG | dashboard | ~slow append (mild risk) |
| `customer_acquisition_log` | New-customer acquisition events | generator (`maybe_acquire_customer`) | audit DAG | slow |
| `ml_customer_scores` | Per-customer ML anomaly + triage scores (migration `019`) | `ml_score` DAG (`src/ml/train.py`, every 6h) | dashboard (Risk Models / Scenarios), SQL Explorer | overwrite per run (~active customers) |
| `ml_model_runs` | ML model metadata + eval metrics (ROC-AUC, PR-AUC) | `ml_score` DAG | dashboard, System Health | 1 row/run |
| `event_log` | Central WARNING+ event/error log (migration `020`) | all services via `PostgresLogHandler` + Airflow `on_failure_callback` | Logs page, System Health | slow append |

### Why `account_window_metrics` grew to 121M rows in 3 weeks — exact code path

This is the headline incident. Trace it:

1. Spark's `process_batch` calls `compute_window_metrics(df, ...)` every 30s (`streaming_job.py:292`).
2. `window_engine.compute_window_metrics` (`window_engine.py:54`) reads **30 days of history** from `aml.transactions` (line 62), unions it with the current batch, then loops over **4 window types** (`daily/weekly/biweekly/monthly`, `WINDOWS` dict line 27) and does a `groupBy("sender_id")` aggregation for each. So each batch emits **(distinct senders in 30-day window) × 4 rows**.
3. The result is written by `_write_metrics_upsert` (`streaming_job.py:145`).

**The original bug:** the write was effectively **append-only per batch**. The dedup key is `(customer_id, window_type, window_start)`. The original `_window_start` returned a *sliding* timestamp (`return now - delta`, still present as the fallback at `window_engine.py:51`). Because `now` changes every batch, `window_start` was different on every 30s tick → the `ON CONFLICT (customer_id, window_type, window_start)` key **never collided** → every batch INSERTed brand-new rows instead of updating.

**The math** (documented in `.env:45`): with ~120 batches/hour × 4 windows × ~500 distinct senders appearing in the rolling 30-day union ≈ **~240,000 rows/hour, append-only**. Over 3 weeks: `240k × 24 × 21 ≈ 121M rows`. Combined with the retention bug (§6/§10), nothing was deleting them → table bloat filled the disk.

**The fix that's now in the code:** `_window_start` (`window_engine.py:35`) snaps each window to a **stable calendar boundary** (start of day / ISO week / fixed biweekly epoch / first of month), so `window_start` is constant within a period. Combined with the genuine **upsert** (`INSERT ... ON CONFLICT ... DO UPDATE`, line 159) and the `UNIQUE (customer_id, window_type, window_start)` constraint (`init.sql:102`), each customer now has at most 4 rows per period that get *updated* in place. Live count confirms it: ~6k rows, not 121M.

---

# 5. Kafka

- **Topics:** one — `transactions.raw` (`KAFKA_TOPIC_RAW`), 1 partition / replication factor 1, auto-created by the generator's `ensure_topic` (`transaction_generator.py:464`) or `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
- **Producer:** `transaction-gen` only (`publish`, line 487), `acks=1`, flush per message.
- **Consumer:** `spark-job` only (`spark.readStream...subscribe`, `streaming_job.py:327`), offsets checkpointed in `/tmp/spark-checkpoints-enterprise-v4`.
- **Role:** decoupling/replayable buffer between ingestion and processing — if Spark restarts, offsets/checkpoint let it resume; if the generator dies, messages persist. Listeners: internal `kafka:29092` only (no host port; not exposed to the internet). The ~1.1 GB ZK+Kafka cost is a deliberate "industry-standard" trade-off on a 6 GB VM (see `EVALUATION.md` §3).

---

# 6. Airflow DAGs

All in `orchestration/dags/`, **SequentialExecutor**, `start_date=2026-01-01`, `catchup=False`. Every DAG has an `on_failure_callback` (`_event_log.log_dag_failure`) that records failures to `aml.event_log`.

| DAG | Schedule | What it does |
|---|---|---|
| `cleanup_dag` | `@hourly` | Lifecycle retention for transactions/metrics (see below) |
| `disk_guard_dag` | `*/15` | Host disk + PG size monitor; emergency `TRUNCATE` of metrics |
| `aml_pipeline_audit` | `@hourly` | Inserts alert-by-rule + customer-universe counts into `pipeline_metrics` |
| `dbt_transform` | `@daily` | `dbt run && dbt test` for staging+gold |
| `trigger_sar` | `*/30` | Runs `python -m src.ai_models.sar_generator` |
| `ml_score` | `0 */6 * * *` (every 6h) | Trains/refreshes the ML risk layer (`src/ml/train.py`) → `aml.ml_customer_scores` + `aml.ml_model_runs` |

(The legacy `cleanup_raw_data` DAG was deleted along with `aml.raw_transactions`.)

**Cleanup / retention logic** lives in `cleanup_dag.cleanup_lifecycle` (`cleanup_dag.py`):
- `DELETE FROM aml.transactions WHERE ingested_at < now()-90d` (`TXN_RETENTION_DAYS`, from `configs/retention.json`)
- `DELETE FROM aml.account_window_metrics WHERE computed_at < now()-4h` (`METRICS_RETENTION_HOURS`)
- Then a **row-cap overflow** delete: if remaining metrics > `METRICS_MAX_ROWS=50,000`, delete oldest-by-`computed_at` down to the cap.

**Why the cleanup DAG was deleting nothing (the incident):**
- **Original misconfiguration:** retention for the metrics/transactions was set to a long horizon (the **90-day** `TXN_RETENTION_DAYS`, originally reused for metrics). On a system only ~3 weeks old, `cutoff = now() - 90 days` is *older than any row that exists*, so `DELETE ... WHERE computed_at < cutoff` matched **0 rows** while the table appended 240k rows/hour. Retention was structurally incapable of catching up.
- **Subtle bug that still exists in the current "fixed" version:** because `_write_metrics_upsert` sets `computed_at = EXCLUDED.computed_at` (current timestamp) on **every** update, an actively-transacting customer's metric row is perpetually "fresh." So the age-based rule `computed_at < now()-4h` only ever removes rows for customers who went quiet >4h ago — it can't bound a hot table. The thing actually keeping the table small today is the **row-cap** path (and the disk guard's `TRUNCATE`), not the age rule — pruning by closed-window `window_start` is the cleaner long-term fix.

---

# 7. Spark

**What the job does** (`src/processing/streaming_job.py`): Structured Streaming, reads Kafka `transactions.raw`, and for each 30s micro-batch (`process_batch`, line 236):
1. `_normalize_batch` — coalesce/fill sender/receiver/branch/country/amount_eur.
2. `enrich_with_customer_context` — JDBC-reads the **full** `aml.customers` table + a `DISTINCT ON` over `customer_addresses`, joins to the batch.
3. `compute_customer_behavior_30d` — reads 30 days of history, aggregates per customer, **overwrites** `aml.customer_behavior_30d` (`mode("overwrite")`, line 260).
4. Writes the batch to `aml.transactions` (append).
5. `compute_window_metrics` — 30-day history read + 4-window aggregation; `.collect()`s rows to the driver and upserts into `account_window_metrics`.
6. `evaluate_rules` — applies 8 AML rules (geographic, high_value, smurfing, daily_velocity, weekly_volume, monthly_peer_anomaly, dormant_reactivation, mule_inbound) only to scenario-flagged rows (`_scenario_only`), one flag per customer per rule.
7. `apply_alert_budget` — enforces `DAILY_ALERT_CAP=100` and `PER_RULE_DAILY_CAP=12`.
8. `_with_alert_priority` — re-reads `customers`, blends 60% rule signal + 40% KYC risk → writes `flagged_transactions`.

**Reads:** Kafka topic; `aml.transactions` (30-day history, twice — metrics + behavior); full `aml.customers` (three times per batch: enrichment, dormant check, priority); `customer_addresses`; `flagged_transactions` (budget counts). **Writes:** `aml.transactions`, `account_window_metrics`, `customer_behavior_30d`, `flagged_transactions`.

**Why it was crash-looping for 2 weeks (and is still fragile):** I confirmed the live symptom in the running container — **every batch overruns its trigger**:
```
WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 30000 ms, but spent 43341 ms
```
Root causes:
- **Per-batch full/large JDBC scans against a 1.23M-customer DB**: each 30s batch reads all of `customers` (235 MB) up to 3×, plus two 30-day history scans of `transactions`, plus a full overwrite of `customer_behavior_30d`. That's far more than 30s of work, so batches queue up.
- **`.collect()` to a 512 MB driver** (`_write_metrics_upsert`, line 150) and `.mode("overwrite")` table rewrites create memory spikes and lock churn.
- With `SPARK_DRIVER_MEMORY=512m` / `SPARK_EXECUTOR_MEMORY=512m` and `local[2]` inside a 1536m container, sustained backpressure + the (original) ever-growing `account_window_metrics` scans pushed the JVM into **OOM**, the container has `restart: unless-stopped`, so it restarted and re-OOM'd → **crash loop**. The metrics bloat made the history/upsert path progressively heavier, which is why it worsened over ~2 weeks. It currently survives but is permanently "falling behind."

---

# 8. Streamlit

`src/dashboard/app.py` is an enterprise compliance UI (dark theme, i18n EN/DE/TR via `locales/`). 11 pages (from `nav_pages` in `app.py`):
- **Overview** (`onboarding.py`): project intro + medallion architecture diagram (Kafka bronze → `aml.transactions` silver → gold).
- **Monitoring** (`page_monitoring`): KYC KPI row, risk-band pie, alerts-by-rule bar, live recent alerts, paginated live transaction feed.
- **Investigation** (`investigation.py`): drill into an alert (`txn_id`+`rule`) or a customer — profile, addresses, 30-day transactions, behavior, window metrics, open alerts. The "top high-risk alerts" panel sits at the top in idle mode and collapses to the bottom when a specific case is open.
- **SAR Archive** (`sar_archive.py` / `sar_reporter.py`): view/generate SAR drafts.
- **Scenarios** (`rule_builder_ui.py`): **read-only** showcase of the 8 rule-based typologies + an always-on ML anomaly card (the 9th "scenario"); shows each rule's window and live thresholds (no editing).
- **Risk Models** (`risk_models.py`): ML model performance — score distribution, ROC/PR curves, feature importance, rule-vs-ML overlap, with plain-language explainers.
- **Analytics** (`analytics.py`): transaction/alert trends, customer acquisition, segmentation, cross-border corridors, and risk-band/PEP incidence (measured against active customers).
- **Data Quality** (`data_quality.py`): rule-based DQ checks (`configs/data_quality_rules.json`).
- **System Health** (`system_health.py`): per-service freshness/health cards (ingest, scheduler, dbt, acquisition, SAR, ML) + monitored-table row counts.
- **Logs** (`event_log.py`): central `aml.event_log` viewer — summary tiles, level/source/time filters, trend charts.
- **SQL Explorer** (`freesql.py`): **read-only** ad-hoc SQL (sanitized, `freesql_reader` SELECT-only) with example queries incl. an ML category.

**Data source:** all queries go through `src/dashboard/db.py` via SQLAlchemy directly against Postgres `aml.*` (e.g. `fetch_open_alerts`, `fetch_recent_alerts`, `fetch_customer_profile`, `fetch_alerts_by_rule_24h`, `fetch_risk_band_distribution`, `fetch_sar_reports`). "Open alerts" = `flagged_transactions` left-anti-joined against `alert_dispositions`. It reads gold/silver operational tables (plus `ml_*` and `event_log`); it does not query Kafka or Spark directly.

---

# 9. SAR Worker

- **SAR = Suspicious Activity Report** — the regulatory filing a bank submits about suspected money laundering.
- **What it does** (`src/ai_models/sar_generator.py`): groups `flagged_transactions` by `account_id`, aggregates count/rules/max amount (`_fetch_flagged_groups`, line 49), skips accounts that already have a SAR (dedup by `account_id_hash`), and drafts a report — either a **mock template** (`_mock_sar`, default `SAR_MOCK_MODE=true`) or via **OpenAI** (`_openai_sar`, `gpt-4o-mini`) when a key is set. PII is protected: only a SHA-256 hash of the account id is stored. Results go to `aml.sar_reports` (`_insert_sar`).
- **Triggers:** two paths — the `sar-worker` container runs `run_loop(interval_sec=300)` (every 5 min), and the Airflow `trigger_sar` DAG runs the same module every 30 min. It only acts on *new* flagged account groups (no duplicate SARs).

---

# 10. Data Retention & Cleanup

**Current policy** (`.env`, `cleanup_dag.py`, `disk_guard_dag.py`):

| Data | Retention | Mechanism |
|---|---|---|
| `transactions` (silver landing) | 90 days by `ingested_at` | `cleanup_dag` hourly |
| `account_window_metrics` (silver) | 4 hours by `computed_at` **+ 50k row cap** | `cleanup_dag` hourly |
| metrics emergency | TRUNCATE if disk ≥90% or rows >200k | `disk_guard_dag` every 15 min + `scripts/disk_guard.sh` cron |
| `flagged_transactions` / `sar_reports` | kept (compliance) | none |

**What was broken:** (1) the original metrics retention horizon was far too long (90 days) for a 3-week-old, fast-appending table → deleted nothing; (2) the metrics write was append-only (no working upsert/window alignment) → 240k rows/hour; (3) the age-based metrics delete is fundamentally weak because the upsert keeps refreshing `computed_at` (so only the row-cap really bounds it).

**What the intervals *should* be, given ~240k metric-rows/hour pre-fix and ~5–8k txns/day post-fix:**
- Metrics: with the upsert fix, the natural size is `≈ active customers × 4 windows`. Keep the **row cap as the primary control** (e.g. 50k–100k) and make the age rule prune by **`window_start` of *closed* windows** (e.g. delete daily windows older than ~2 days, weekly older than ~2 weeks), not `computed_at`. A 4-hour `computed_at` rule is fine as a secondary sweep but shouldn't be the main bound.
- Transactions: 30 days is reasonable for the rolling-window features (the engine reads 30 days). If disk is tight, 14 days still covers daily/weekly/biweekly windows.
- `pipeline_metrics`: currently append-only with no retention — add a 30–90 day prune (small, but unbounded).

---

# 11. Machine Learning risk layer

The ML layer (`src/ml/`) is an **always-on "9th scenario"**: it runs alongside the eight rule typologies but is **descriptive, not alerting** — it never writes to `aml.flagged_transactions`. Its job is to (a) surface anomalous customers the static thresholds miss, and (b) benchmark the rule engine with a supervised model. It is scheduled by the `ml_score` Airflow DAG every 6 hours (`0 */6 * * *`) and is intentionally cheap: it trains on a few-thousand-row, active-sender feature frame, not the 1.23M universe.

### 11.1 Feature engineering (`src/ml/features.py`)

One row per **active sending customer** over a rolling **30 days**, built directly from the live operational tables (`aml.transactions` + `aml.customers` + `aml.flagged_transactions`) — deliberately the same behavioral surface the rule engine sees. Twelve model features:

| Feature | Meaning |
|---|---|
| `txn_count_30d` | number of outbound transactions |
| `volume_30d` | total outbound amount (EUR) |
| `avg_amount_30d`, `max_txn_30d`, `std_amount_30d` | amount distribution shape |
| `distinct_receivers_30d` | fan-out (mule / smurfing signal) |
| `distinct_countries_30d` | geographic spread |
| `cross_border_txns`, `cross_border_ratio` | non-DE receiver count and its share |
| `fast_txns` | count of `FAST`-type transfers (velocity signal) |
| `kyc_risk_score`, `is_pep` | static KYC context joined from `customers` |

Monetary/heavy-tailed columns (`volume_30d`, `avg_amount_30d`, `max_txn_30d`, `std_amount_30d`) get a **`log1p`** transform so a handful of large transfers don't dominate. The label `rule_flagged = flag_count_30d > 0` is computed but **excluded from the input matrix** to avoid leakage — the supervised model must *predict* the alert, not read it.

### 11.2 Models (`src/ml/train.py`)

1. **Unsupervised — Isolation Forest** (`n_estimators=200`, `contamination=0.05`, `random_state=42`). The raw `-decision_function` is min-max **normalized to `anomaly_score` ∈ [0,1]** (higher = more anomalous); `is_anomaly = predict() == -1`; customers are ranked into `anomaly_rank`. Always produced, even on sparse data.
2. **Supervised — Gradient Boosting triage**, trained on the `rule_flagged` label, but only when there are **≥ 10 positives and ≥ 10 negatives** (`ML_MIN_PER_CLASS`) and **≥ 40 samples** (`ML_MIN_SAMPLES`). Because labels are scarce and imbalanced, evaluation uses **stratified K-fold out-of-fold probabilities** (`cross_val_predict`), not a tiny single holdout — every positive is used for scoring. Reported metrics: **ROC-AUC, PR-AUC**, and **precision/recall/F1 at the F1-optimal threshold** (not a fixed 0.5). The model is then refit on the full set to emit stable `feature_importance` and a per-customer `triage_score`. If the label bar isn't met, the run records *why* supervision was skipped and still ships anomaly scores — honest by design.

### 11.3 Persistence & surfacing

- **`aml.ml_customer_scores`** — current snapshot only (the job `DELETE`s then re-inserts): `customer_id, model_version, anomaly_score, anomaly_rank, is_anomaly, triage_score, rule_flagged` + a few context columns (`txn_count_30d`, `volume_30d`, `distinct_receivers_30d`, `max_txn_30d`, `kyc_risk_score`).
- **`aml.ml_model_runs`** — one row per run: sample/feature/anomaly counts, `contamination`, `positive_rate`, `roc_auc`, `pr_auc`, `precision/recall/f1`, the downsampled `roc_curve`/`pr_curve` and `feature_importance` as JSONB, plus free-text `notes`.
- A best-effort **joblib** artifact (`aml_risk_models.joblib`) is written to `MODEL_DIR` (never fails the job).
- Surfaced in the dashboard **Risk Models** page (distribution, ROC/PR curves, feature importance, rules-vs-ML overlap), the **Scenarios** ML card (9th card, live run stats), the **System Health** ML card (freshness), and the **SQL Explorer** ML query category.

---

# 12. End-to-end transaction lifecycle

Tracing **one transaction** from birth to (possible) SAR makes the whole system concrete:

1. **Generation** (`transaction-gen`, every 10–20 s). Either a *normal* payment — a realistic enterprise payload (`_txn_payload`) with sender/receiver customer numbers, branch, country, multi-currency amount FX-converted to EUR, capped under `NORMAL_MAX_AMOUNT_EUR=8500` so it never trips a rule — or, when the `ScenarioScheduler` wakes, a *scenario burst* injected by `scenario_injectors.py` that is engineered to cross a specific threshold.
2. **Kafka** (`transactions.raw`). The dict is serialized to JSON and `produce()`d with `acks=1`. The topic is the bronze layer; if Spark is down, messages wait (bounded by Kafka retention).
3. **Spark micro-batch** (`spark-job`, every 30 s, `startingOffsets=latest`, `failOnDataLoss=false`). `process_batch` runs the full chain:
   - `_normalize_batch` — coalesce/fill sender/receiver/branch/country/amount.
   - `enrich_with_customer_context` — JDBC-join `aml.customers` + a `DISTINCT ON` of `customer_addresses`.
   - Write to **`aml.transactions`** (append; `ingested_at = now()`, distinct from event `ts`).
   - `compute_window_metrics` — read 30 days of history, union the batch, aggregate over **4 windows** (daily/weekly/biweekly/monthly), **upsert** into `account_window_metrics` (calendar-aligned `window_start`, `ON CONFLICT DO UPDATE`).
   - `compute_customer_behavior_30d` — overwrite the 30-day rolling profile.
   - `evaluate_rules` — apply the **8 AML rules** only to scenario-flagged rows (`_scenario_only`), one flag per customer per rule.
   - `apply_alert_budget` — enforce `DAILY_ALERT_CAP=100` and `PER_RULE_DAILY_CAP=12`.
   - `_with_alert_priority` — blend **60 % rule signal + 40 % KYC risk** → write **`aml.flagged_transactions`** (`flagged_at = now()`).
4. **Dashboard** reads it live within seconds — the Monitoring feed, the recent-alerts panel, and (if flagged) the open-alerts queue and Investigation drill-down.
5. **SAR** (`sar-worker`, every 300 s; `trigger_sar` DAG every 30 min as a second path). New flagged **account groups** (deduped by `account_id_hash`, PII hashed) are drafted into **`aml.sar_reports`** — mock template by default, OpenAI `gpt-4o-mini` when a key is set.
6. **ML** (`ml_score`, every 6 h). The customer's updated 30-day behavior re-enters the feature frame and gets a fresh `anomaly_score` / `triage_score`.
7. **Batch & lifecycle** (Airflow). `aml_pipeline_audit` snapshots counts hourly; `dbt_transform` rebuilds gold daily; `cleanup_dag` prunes `aml.transactions` after 90 days; `disk_guard_dag` watches disk every 15 min.

The three timestamps — event `ts` (when it happened), `ingested_at` (when Spark landed it), `flagged_at` (when a rule fired) — form the audit trail that separates ingestion time from processing time.

---

# 13. dbt models in detail

dbt provides the warehouse/lineage layer on top of the operational `aml.*` tables, run by the `dbt_transform` DAG (`@daily`, `dbt run && dbt test`).

- **Sources** (`models/sources.yml`) — the operational tables Spark writes (`aml.transactions`, `aml.flagged_transactions`, `aml.customers`, …) declared as dbt sources so every downstream model uses `source()`/`ref()` lineage.
- **Staging** (`models/staging/`) — `stg_flagged_transactions` cleans and types the alert stream as a view, with `schema.yml` data tests (`not_null`, `unique`, `accepted_values`). (The legacy `stg_raw_transactions` was retired with `aml.raw_transactions` in migration `021`.)
- **Gold** (`models/gold/`) — three reporting/risk tables:
  - `gold_daily_fraud_summary` — flags aggregated per day and per rule (trend/throughput reporting).
  - `gold_account_risk_score` — buckets accounts into high/medium/low by flag count.
  - `gold_customer_risk_profile` — joins transactions + customers + flags into a 30-day per-customer risk profile (has its own `gold_customer_risk_profile_schema.yml` tests).

The medallion mapping is therefore: **bronze** = Kafka `transactions.raw`; **silver** = `aml.transactions` + Spark's operational aggregates + the dbt staging view; **gold** = the three dbt gold tables.

---

# 14. Configuration & rules reference

All detection thresholds live in version-controlled JSON under `configs/` — a single source of truth shared by the Spark engine and the dashboard.

**`configs/rules.json`** — the eight rule thresholds:

| Rule | Key thresholds |
|---|---|
| `velocity` | `window_minutes=5`, `max_txns_per_account=5` |
| `high_value` | `threshold_eur=10000` |
| `geographic` | `blocked_countries=[IR,KP,SY,CU,RU]`, `high_risk_countries=[RU,KP]` |
| `daily_velocity` (multi_window) | `daily_velocity_max=5` each ≤ `1000` EUR |
| `weekly_volume` (multi_window) | `weekly_volume_max_eur=10000` |
| `biweekly` distinct receivers | `biweekly_distinct_receivers_max=20` |
| `monthly_peer_anomaly` | `multiplier=2.5` × `baseline_txn_count=8` (≈ >20/month) |
| `smurfing` | `weekly_small_txn_threshold_eur=500`, `count=12` |
| `dormant_reactivation` | `min_amount_eur=3000` on a dormant account |
| `mule_inbound` | `window_hours=24`, `min_distinct_senders=5`, `min_total_amount_eur=500` |

**`configs/scenario_catalog.json`** — the synthetic-injection catalog. Seven scenarios are actively injected (geographic, high_value, smurfing, daily_velocity, weekly_volume, dormant_reactivation, mule_inbound); `monthly_peer_flood` is `enabled:false` (detected organically). The scheduler honors `SCENARIO_WAKE_MIN/MAX_SEC`, `SCENARIO_DAILY_CAP`, per-rule cooldown, and an empty-result cooldown so no unproductive scenario can monopolize the slot.

**`configs/retention.json`** — lifecycle source of truth: `transactions` 90 d, `flagged_transactions` 180 d, `sar_reports` 180 d, `account_window_metrics` 4 h + 50k row cap. **`configs/data_quality_rules.json`** drives the Data Quality page checks.

---

# 15. Security & deployment

- **Edge / HTTPS** (`edge` compose profile). A **Caddy** reverse proxy terminates TLS with an **automatic Let's Encrypt** certificate for `utku-efe-aml.duckdns.org`, redirects `http → https`, and proxies to `streamlit:8501` over the internal Docker network. A **DuckDNS** updater keeps the A-record pointed at the VM. Live at **https://utku-efe-aml.duckdns.org**.
- **Network posture.** The only internet-facing ports are Caddy's **80/443**. Streamlit is bound to `127.0.0.1:8501` (reachable only via SSH tunnel / the proxy); **Postgres and Kafka have no host ports** — they live entirely on the `aml-net` Docker bridge. The Oracle Cloud Security List allows only 22/80/443.
- **Read-only SQL Explorer.** Queries run as a dedicated `freesql_reader` role with **SELECT-only** grants, behind a regex sanitizer that blocks DDL/DML — so the public dashboard cannot mutate data.
- **PII.** The SAR worker stores only a **SHA-256 hash** of the account id, never raw identifiers.
- **Secrets.** Credentials and the optional OpenAI key live in `.env`, which is **git-ignored** (only `.env.example` is committed). The DB password is still the default `password`; now that the DB is internal-only this is defense-in-depth rather than an exposed hole, but rotating it is the recommended next step. `EVALUATION.md` flags moving secrets to Vault / Oracle Secrets.

---

# 16. Observability

- **Central event log** (`aml.event_log`, migration `020`). A `PostgresLogHandler` (`src/common/event_log.py`) mirrors every `WARNING`+ record from all Python services — generator, Spark driver, SAR worker, ML job and the dashboard — into one table (`ts, source, level, logger, message, detail JSONB`). Airflow DAGs add an `on_failure_callback` (`log_dag_failure`) so task failures land there too.
- **Logs page** (Streamlit) — summary tiles, level/source/time-range/search filters, trend and source-breakdown charts, and a detail table with JSONB expanders.
- **System Health page** — per-service freshness cards (ingest, scenario scheduler, dbt, customer acquisition, SAR, **ML**) graded by age thresholds, plus row counts for the monitored `aml.*` tables (including the ML tables).
- **Disk guard** — `disk_guard_dag` (`*/15`) and `scripts/disk_guard.sh` watch host disk + PG size and emergency-truncate the fastest-growing table before space runs out; `aml_pipeline_audit` snapshots counts hourly into `pipeline_metrics`.