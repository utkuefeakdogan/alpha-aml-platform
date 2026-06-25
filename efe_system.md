I've now read every service, the schema and migrations, the DAGs, dbt, the dashboard, and confirmed runtime behavior against the live database. Here is the complete walkthrough.

A quick but important framing note before the 12 sections: **the code in the repo right now is the post-incident, largely-remediated state.** The live DB confirms recovery — `account_window_metrics` is back down to ~6,000 rows, disk is at 47%, and all containers are healthy. So for each incident I'll explain the original root cause *and* point out what's already been fixed in the current files versus what's still weak. The repo is not under git, so there's no history to diff — I'm reading the present tree plus the self-documenting clues left in `.env` and code comments.

---

# 1. System Architecture

**What it is:** "Alpha AML Pipeline" — a real-time **Anti-Money-Laundering / fraud-detection platform** for a simulated European bank. It generates synthetic banking transactions, streams them through Kafka, applies windowed AML rules in Spark, persists everything in PostgreSQL, transforms raw→silver→gold with dbt, surfaces alerts in a Streamlit compliance dashboard, and auto-drafts **SAR** (Suspicious Activity Reports) with a GenAI worker. It's explicitly built as a senior/lead data-engineering interview showcase tuned for a 6 GB Oracle Cloud free-tier VM (see `README.md`, `EVALUATION.md`).

**End-to-end:** `transaction-gen` → Kafka topic `transactions.raw` → `spark-job` (30s micro-batches) → Postgres `aml.*` → dbt (gold models) / Streamlit / `sar-worker`, with Airflow orchestrating retention, audits, dbt runs, SAR triggers, and disk guarding.

**Services** (from `docker-compose.yml`, grouped by profile):

| Service | Profile | Mem limit | Role |
|---|---|---|---|
| `zookeeper` | core | 384m | Kafka coordination |
| `kafka` | core | 768m | Event bus |
| `postgres` | core, ops | 512m | System of record (`aml` schema) |
| `transaction-gen` | app | 256m | Synthetic transaction producer |
| `spark-job` | app | 1536m | Structured Streaming rule engine |
| `streamlit` | app | 384m | Compliance dashboard (port 8501) |
| `sar-worker` | app | 256m | GenAI SAR generator (300s loop) |
| `airflow-init` / `airflow` | ops | 512m each | LocalExecutor scheduler + DAGs |

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
        │  cleanup_dag / cleanup_raw_data (retention)   dbt_transform (gold)                         │
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
- **Fraud injection:** a `ScenarioScheduler` (`scenario_scheduler.py`) wakes on a random interval (`SCENARIO_WAKE_MIN/MAX_SEC` = 1200–5400s) and injects one of 8 scenarios from `configs/scenario_catalog.json` (geographic, high_value, smurfing, daily_velocity, weekly_volume, monthly_peer, dormant_reactivation, mule_inbound) via `scenario_injectors.py`. It self-limits to `SCENARIO_DAILY_CAP=100` alerts/day and prioritizes rules with no alerts in the last 7 days. Normal transactions are amount-capped at `NORMAL_MAX_AMOUNT_EUR=8500` (`_cap_normal_amount`, line 210) so only injected scenarios cross thresholds.

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
| **Raw / ingestion** | Every transaction event as landed from Kafka | `aml.transactions` (enterprise) and legacy `aml.raw_transactions` | `spark-job` (`_write_jdbc(txn_write, "aml.transactions")`, line 286) | Spark / Postgres |
| **Silver (operational)** | Per-customer multi-window aggregates + 30d behavior + flagged alerts | `aml.account_window_metrics`, `aml.customer_behavior_30d`, `aml.flagged_transactions` | `spark-job` (`window_engine.compute_window_metrics`, `customer_enrichment.compute_customer_behavior_30d`, `evaluate_rules`) | Spark |
| **Silver (dbt staging)** | Cleaned views + data tests | dbt `staging` schema views `stg_raw_transactions`, `stg_flagged_transactions` | dbt | dbt |
| **Gold** | Reporting/risk aggregates | dbt `gold` schema tables: `gold_daily_fraud_summary`, `gold_account_risk_score`, `gold_customer_risk_profile` | dbt (`dbt_transform` DAG) | dbt |

- **Raw** = the immutable transaction stream. `ingested_at`/`flagged_at` ≠ event `ts`, preserving an audit trail.
- **Silver** = Spark's stateful aggregations and rule outputs (the operational AML state).
- **Gold** = business-facing summaries: e.g. `gold_account_risk_score.sql` buckets accounts into high/medium/low by flag count; `gold_daily_fraud_summary.sql` aggregates flags per day/rule; `gold_customer_risk_profile.sql` joins transactions+customers+flags for a 30-day risk profile.

Note: `dbt/models/staging/stg_raw_transactions.sql` reads `raw_transactions` (the *legacy* table), which is essentially empty in this deployment — a small lineage inconsistency, since live data lands in `aml.transactions`.

---

# 4. PostgreSQL Schema (`aml`)

Defined in `docker/init.sql` + migrations `002`–`014`. Live tables and current sizes (queried just now):

| Table | Purpose | Written by | Read by | Growth |
|---|---|---|---|---|
| `customers` | KYC master / Customer 360 (risk_score, segment, PEP, status, country) | migrations (seed 250; `011` scales to **1,233,000**) | Spark enrichment, generator sampling, dashboard, dbt | Static (235 MB) |
| `customer_addresses` | 1–5 addresses per customer (migration `013`) | migrations | Spark enrichment, dashboard | Static (~3.7M rows, 445 MB) |
| `transactions` | **Raw** enterprise transactions | Spark `_write_jdbc(...,"aml.transactions")` | metrics/behavior history reads, dashboard, dbt | ~5–8k/day (10 MB now) |
| `raw_transactions` | Legacy raw table (lineage) | (none in current pipeline) | dbt staging | ~empty |
| `account_window_metrics` | **Silver** per-customer aggregates × 4 windows | Spark `_write_metrics_upsert` | rule engine, dashboard, disk guard | **bounded now (~6k)**; was 121M |
| `customer_behavior_30d` | 30-day rolling profile per customer | Spark `.mode("overwrite")` each batch | dashboard | bounded (~6.6k) |
| `flagged_transactions` | AML alerts (rule, detail, window, priority) | Spark `_write_jdbc(...,"aml.flagged_transactions")` | dashboard, SAR, dbt, audit | slow (capped ~100/day) |
| `alert_dispositions` | Analyst decisions (FP / SAR filed) | Streamlit `save_alert_disposition` | dashboard | slow (human) |
| `sar_reports` | Generated SAR drafts | `sar-worker`, dashboard | dashboard | slow |
| `pipeline_metrics` | Hourly audit snapshots (migration `012`) | `aml_pipeline_audit` DAG | dashboard | ~slow append (mild risk) |
| `customer_acquisition_log` | New-customer acquisition events | generator (`maybe_acquire_customer`) | audit DAG | slow |

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
- **Role:** decoupling/replayable buffer between ingestion and processing — if Spark restarts, offsets/checkpoint let it resume; if the generator dies, messages persist. Listeners: internal `kafka:29092`, host `localhost:9092`. (`EVALUATION.md` §2 acknowledges this ~1.1 GB ZK+Kafka cost is a deliberate "industry-standard" tradeoff on a 6 GB VM.)

---

# 6. Airflow DAGs

All in `orchestration/dags/`, LocalExecutor, `start_date=2026-01-01`, `catchup=False`.

| DAG | Schedule | What it does |
|---|---|---|
| `cleanup_dag` | `@hourly` | Lifecycle retention for transactions/raw/metrics (see below) |
| `cleanup_raw_data` | `@hourly` | Deletes `raw_transactions` older than `RAW_RETENTION_HOURS=24` (legacy/duplicate of part of `cleanup_dag`) |
| `disk_guard_dag` | `*/15` | Host disk + PG size monitor; emergency `TRUNCATE` of metrics |
| `aml_pipeline_audit` | `@hourly` | Inserts alert-by-rule + customer-universe counts into `pipeline_metrics` |
| `dbt_transform` | `@daily` | `dbt run && dbt test` for staging+gold |
| `trigger_sar` | `*/30` | Runs `python -m src.ai_models.sar_generator` |

**Cleanup / retention logic** lives in `cleanup_dag.cleanup_lifecycle` (`cleanup_dag.py:19`):
- `DELETE FROM aml.transactions WHERE ingested_at < now()-30d` (`TXN_RETENTION_DAYS`)
- `DELETE FROM aml.raw_transactions WHERE ingested_at < now()-24h` (`RAW_RETENTION_HOURS`)
- `DELETE FROM aml.account_window_metrics WHERE computed_at < now()-4h` (`METRICS_RETENTION_HOURS`)
- Then a **row-cap overflow** delete: if remaining metrics > `METRICS_MAX_ROWS=50,000`, delete oldest-by-`computed_at` down to the cap.

**Why the cleanup DAG was deleting nothing (the incident):**
- **Original misconfiguration:** retention for the metrics/transactions was set to a long horizon (the **90-day** value in your hint, likely `TXN_RETENTION_DAYS` reused for metrics). On a system only ~3 weeks old, `cutoff = now() - 90 days` is *older than any row that exists*, so `DELETE ... WHERE computed_at < cutoff` matched **0 rows** while the table appended 240k rows/hour. Retention was structurally incapable of catching up.
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

`src/dashboard/app.py` is an enterprise compliance UI (dark theme, i18n EN/DE/TR via `locales/`). Pages (from `app.py` + module imports):
- **Monitoring** (`page_monitoring`): KYC KPI row, risk-band pie, alerts-by-rule bar, live recent alerts, paginated live transaction feed.
- **Investigation** (`investigation.py`): drill into an alert (`txn_id`+`rule`) or a customer — profile, addresses, 30-day transactions, behavior, window metrics, open alerts.
- **Rule Builder** (`rule_builder_ui.py` / `rules_manager.py`): manage `rules.yaml`/`rules.json` thresholds and the scenario catalog.
- **SAR Archive** (`sar_archive.py` / `sar_reporter.py`): view/generate SAR drafts.
- **Data Quality** (`data_quality.py`): rule-based DQ checks (`configs/data_quality_rules.json`).

**Data source:** all queries go through `src/dashboard/db.py` via SQLAlchemy directly against Postgres `aml.*` (e.g. `fetch_open_alerts`, `fetch_recent_alerts`, `fetch_customer_profile`, `fetch_alerts_by_rule_24h`, `fetch_risk_band_distribution`, `fetch_sar_reports`). "Open alerts" = `flagged_transactions` left-anti-joined against `alert_dispositions`. It reads gold/silver/raw operational tables; it does not query Kafka or Spark directly.

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
| `transactions` (raw) | 30 days by `ingested_at` | `cleanup_dag` hourly |
| `raw_transactions` (legacy) | 24 hours | `cleanup_dag` + `cleanup_raw_data` |
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
| **Airflow OOM-killed** | `airflow` container at 512m running LocalExecutor scheduler+webserver+tasks; the `disk_guard`/`cleanup` tasks query a multi-GB DB and the bloated metrics table; concurrent with full `core+app` stack on 6 GB | Raise `mem_limit` for `airflow` (≥1 GB), keep `ops` profile separate from heavy `app` load (the runbook in `EVALUATION.md` §1.3 already prescribes not running `ops` simultaneously), limit parallelism. |
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
- **dbt staging inconsistency:** `stg_raw_transactions` reads the near-empty legacy `raw_transactions`; gold models that depend on the real stream should source `aml.transactions`. Align the source mapping in `sources.yml`/staging.
- **Two overlapping cleanup DAGs** (`cleanup_dag` vs `cleanup_raw_data`) both delete `raw_transactions` hourly — consolidate to avoid confusion.
- **Secrets:** `.env` holds DB creds and the OpenAI key in plaintext and is committed alongside `.env.example` (`EVALUATION.md` §5 flags moving to Vault).
- **`_write_metrics_upsert` `.collect()`** brings all metric rows to the driver; for large active-customer counts use `foreachPartition` with a partition-local connection, or write to a staging table + server-side `MERGE`.

---

Want me to actually apply any of these fixes? The highest-impact ones are (1) the `cleanup_dag.py` retention rewrite + `pipeline_metrics` pruning and (2) the Spark per-batch read reduction (broadcast customers, move the behavior overwrite out of the stream) to stop the "falling behind" backpressure. I can also bump the compose memory limits. Tell me which and I'll make the edits.