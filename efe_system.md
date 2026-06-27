# Alpha AML — System Walkthrough & Operations Notes

This is my own deep walkthrough of the platform I built: every service, the schema and migrations, the DAGs, dbt, the dashboard, and the runtime behavior I've verified against the live database. I keep it next to [`EVALUATION.md`](EVALUATION.md) (architecture/operations rationale) so I — and anyone reviewing the project — can understand exactly how the system works and why I made each decision.

A framing note before the 12 sections: **the code in the repo is the post-incident, largely-remediated state.** Early on the platform hit a disk/metrics-bloat incident, and I rebuilt the affected paths; the live DB confirms recovery — `account_window_metrics` is back down to ~6,000 rows, disk is healthy, and all containers are healthy. So for each incident I explain the original root cause *and* what I've already fixed in the current files versus what's still worth tightening. The project is on GitHub at `github.com/utkuefeakdogan/alpha-aml-platform`.

> **Update note (current state):** since this walkthrough was first written the project has evolved further — the legacy `aml.raw_transactions` table and its `cleanup_raw_data`/`stg_raw_transactions` lineage were retired (migration `021`; Kafka `transactions.raw` is now the bronze layer); an always-on **ML risk layer** was added (`src/ml/`, `aml.ml_customer_scores` + `aml.ml_model_runs`, `ml_score` DAG every 6h); a **central event log** (`aml.event_log`) plus new dashboard pages (Scenarios, Risk Models, Analytics, System Health, Logs, SQL Explorer) were added; Airflow runs **SequentialExecutor**; Postgres mem limit is **1024m** and Airflow **1200m**; and Postgres/Kafka host ports were closed (internal-only). The sections below note these where relevant.

---

# 1. System Architecture

**What it is:** "Alpha AML Pipeline" — a real-time **Anti-Money-Laundering / fraud-detection platform** for a simulated European bank. It generates synthetic banking transactions, streams them through Kafka, applies windowed AML rules in Spark, persists everything in PostgreSQL, transforms raw→silver→gold with dbt, surfaces alerts in a Streamlit compliance dashboard, and auto-drafts **SAR** (Suspicious Activity Reports) with a GenAI worker. It's explicitly built as a senior/lead data-engineering interview showcase tuned for a 6 GB Oracle Cloud free-tier VM (see `README.md`, `EVALUATION.md`).

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

# 3. Data Layers (Raw / Silver / Gold)

The project maps a medallion architecture onto Postgres + dbt (`EVALUATION.md` §2 table):

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
- **Role:** decoupling/replayable buffer between ingestion and processing — if Spark restarts, offsets/checkpoint let it resume; if the generator dies, messages persist. Listeners: internal `kafka:29092` only (no host port; not exposed to the internet). (`EVALUATION.md` §2 acknowledges this ~1.1 GB ZK+Kafka cost is a deliberate "industry-standard" tradeoff on a 6 GB VM.)

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
- **Subtle bug that still exists in the current "fixed" version:** because `_write_metrics_upsert` sets `computed_at = EXCLUDED.computed_at` (current timestamp) on **every** update, an actively-transacting customer's metric row is perpetually "fresh." So the age-based rule `computed_at < now()-4h` only ever removes rows for customers who went quiet >4h ago — it can't bound a hot table. The thing actually keeping the table small today is the **row-cap** path (and the disk guard's `TRUNCATE`), not the age rule. This is worth tightening (see §12).

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

# 11. Known Issues — root cause + fix

| Issue | Root cause | Fix (status) |
|---|---|---|
| **Disk 100% from `account_window_metrics` bloat** | Append-only metrics write (sliding `window_start`, no effective upsert) + over-long retention → 121M rows | ✅ Already fixed: stable calendar `window_start` + `ON CONFLICT DO UPDATE` upsert + `UNIQUE` constraint + 4h/50k cleanup + disk-guard truncate. Table now ~6k rows. |
| **Spark crash-looping** | Per-batch full-table JDBC scans (`customers` ×3, two 30-day history reads), `.collect()` to 512m driver, full `overwrite` of behavior table; OOM under 512m/`local[2]`; `restart: unless-stopped` re-loops | Partially mitigated (metrics no longer unbounded), but **still falling behind every batch**. Needs: broadcast/cached customer reads, drop the per-batch behavior overwrite, raise trigger interval, raise driver/executor memory or reduce work (see §12). |
| **Airflow OOM-killed** | `airflow` container at 512m (originally LocalExecutor) scheduler+webserver+tasks; the `disk_guard`/`cleanup` tasks query a multi-GB DB and the bloated metrics table; concurrent with full `core+app` stack on 6 GB | ✅ Mitigated: `airflow` `mem_limit` raised to **1200m** and switched to **SequentialExecutor** (one task at a time = lower peak memory); runbook (`EVALUATION.md` §1.3) prescribes not running `ops` simultaneously with heavy `app` load. |
| **Zookeeper + Postgres unhealthy** | Cascading from disk pressure: when `/` hits 100%, Postgres can't write WAL/extend files → healthcheck `pg_isready` fails; memory thrash makes ZK miss heartbeats | Resolving the disk bloat (done) clears this; both are `healthy` now. Add the disk monitor as early warning. |
| **Kafka exited** | ZK unhealthy (`depends_on zookeeper: service_healthy`) + memory pressure on a 768m broker when the VM is swapping | Fix upstream disk/mem; Kafka is `healthy` now. Consider lowering retention/segment sizes if disk is tight. |

Net: the disk/metrics incident is **remediated in code and in the live DB**; the **Spark backpressure and memory tuning are the remaining open problems**.

---

# 12. Suggested Fixes

**a) Tighten `cleanup_dag.py` retention** — make metrics pruning real instead of relying on `computed_at`. Prune closed windows by `window_start`, keep the row-cap, and add `pipeline_metrics` retention. Proposed core change to `cleanup_lifecycle`:

```python
# Prune metrics by the window they describe, not by computed_at (which the upsert keeps fresh).
cur.execute("""
    DELETE FROM aml.account_window_metrics
    WHERE (window_type = 'daily'    AND window_start < %s)
       OR (window_type = 'weekly'   AND window_start < %s)
       OR (window_type = 'biweekly' AND window_start < %s)
       OR (window_type = 'monthly'  AND window_start < %s)
""", (now - timedelta(days=2), now - timedelta(days=14),
      now - timedelta(days=28), now - timedelta(days=62)))

# Keep pipeline_metrics bounded too.
cur.execute("DELETE FROM aml.pipeline_metrics WHERE recorded_at < %s", (now - timedelta(days=30),))
```
Also batch large deletes (`DELETE ... LIMIT` loops) and run `VACUUM (ANALYZE)` so freed space is actually reclaimed — a one-shot `DELETE` on a 121M-row table won't return disk to the OS without vacuum/`pg_repack`.

**b) Disk-usage monitoring DAG** — this already exists as `disk_guard_dag.py` (`*/15`, host disk + PG size + emergency truncate) plus `scripts/disk_guard.sh`. The improvement is to make it *alert before* it truncates (it currently truncates silently at critical) and to emit a `pipeline_metrics` row so the dashboard can chart disk/DB growth over time, plus add a per-table size threshold so `customer_addresses`/`customers` growth is visible.

**c) Tune `docker-compose.yml` memory** for the 6 GB VM:
- `spark-job`: the real fix is *less work per batch*, but also bump `SPARK_DRIVER_MEMORY`/`SPARK_EXECUTOR_MEMORY` from 512m toward 768m–1g (container is 1536m) and increase the trigger interval (e.g. 60s) to stop falling behind.
- `airflow`: raise from 512m to ~1g (it's OOMing); keep `ops` off when `app` is under load.
- Total budget: ZK+Kafka+PG (~1.6 GB) + app (~0.9 GB) + Spark (~1.5 GB) ≈ 4 GB leaves little headroom — hence the runbook's "don't run `ops` simultaneously" rule.

**d) Other improvements I spotted in the code:**
- **Spark per-batch DB reads are the core scalability bug.** `enrich_with_customer_context` and `_with_alert_priority` each read the entire `aml.customers` (1.23M rows); the dormant check reads it again. Read once, **broadcast/cache**, and push filters into SQL. The two 30-day `aml.transactions` history scans (metrics + behavior) should be combined or replaced with incremental state.
- **Drop the per-batch `customer_behavior_30d` overwrite** (`streaming_job.py:254`, `mode("overwrite")` every 30s drops+rewrites the whole table — expensive and a read/write race for the dashboard). Move it to the `@daily`/`@hourly` batch in Airflow, or upsert incrementally.
- **dbt staging inconsistency:** ✅ *Resolved* — the near-empty legacy `aml.raw_transactions` and its `stg_raw_transactions` model were retired (migration `021`); lineage now flows Kafka → `aml.transactions` directly.
- **Two overlapping cleanup DAGs:** ✅ *Resolved* — `cleanup_raw_data` was deleted; `cleanup_dag` is now the single retention DAG.
- **Secrets:** `.env` holds DB creds and the OpenAI key in plaintext. It is **git-ignored** (only `.env.example` is committed), but it's still local plaintext — `EVALUATION.md` §5 flags moving to Vault / Oracle Secrets. Also note the DB password is still the default `password`; now that Postgres/Kafka host ports are closed (internal-only), this is defense-in-depth rather than an exposed hole, but rotating it is recommended.
- **`_write_metrics_upsert` `.collect()`** brings all metric rows to the driver; for large active-customer counts use `foreachPartition` with a partition-local connection, or write to a staging table + server-side `MERGE`.

---

The two highest-impact items still on my list are (1) the `cleanup_dag.py` retention rewrite + `pipeline_metrics` pruning and (2) reducing Spark's per-batch DB reads (broadcast the customer table, move the behavior overwrite out of the stream) to stop the "falling behind" backpressure — both tracked here as deliberate next steps rather than open bugs.