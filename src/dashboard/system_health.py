"""System Health — single-pane operational view of the whole AML platform.

All signals are derived from Postgres (datadb + airflow_meta), so the page works
without Docker socket access: pipeline freshness, Airflow DAG health, source
consistency, throughput and storage.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src.dashboard.db import (
    fetch_airflow_dag_health,
    fetch_alerts_by_rule_24h,
    fetch_consistency_checks,
    fetch_ml_latest_run,
    fetch_system_health,
    fetch_table_counts,
)
from src.dashboard.i18n import t

_HEALTH_TABLES = (
    "aml.transactions",
    "aml.flagged_transactions",
    "aml.sar_reports",
    "aml.alert_transaction_archive",
    "aml.customers",
    "aml.customer_acquisition_log",
    "aml.pipeline_metrics",
    "aml.ml_customer_scores",
    "aml.ml_model_runs",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _age_seconds(ts) -> float | None:
    if ts is None or pd.isna(ts):
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return (pd.Timestamp(_now()) - ts).total_seconds()


def _humanize(age: float | None) -> str:
    if age is None:
        return t("health.never")
    age = int(age)
    if age < 60:
        return t("health.ago_s", n=age)
    if age < 3600:
        return t("health.ago_m", n=age // 60)
    if age < 86400:
        return t("health.ago_h", n=age // 3600)
    return t("health.ago_d", n=age // 86400)


def _status_from_age(age: float | None, ok_max: float, stale_max: float) -> str:
    if age is None:
        return "down"
    if age <= ok_max:
        return "ok"
    if age <= stale_max:
        return "stale"
    return "down"


def _fmt_bytes(n) -> str:
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _status_label(status: str) -> str:
    return t(f"health.status.{status}")


def _service_card(name: str, status: str, value: str, sub: str) -> str:
    return f"""
    <div class="hc-card hc-{html.escape(status)}">
      <div class="hc-head">
        <span class="hc-dot hc-dot-{html.escape(status)}"></span>
        <span class="hc-name">{html.escape(name)}</span>
        <span class="hc-pill hc-pill-{html.escape(status)}">{html.escape(_status_label(status))}</span>
      </div>
      <div class="hc-val">{html.escape(value)}</div>
      <div class="hc-sub">{html.escape(sub)}</div>
    </div>
    """


def _render_service_cards(h: dict, dags: pd.DataFrame) -> None:
    # 1. Ingestion pipeline (gen -> Kafka -> Spark -> Postgres)
    ingest_age = h.get("ingest_age_sec")
    ingest_age = float(ingest_age) if ingest_age is not None and not pd.isna(ingest_age) else None
    ingest_status = _status_from_age(ingest_age, ok_max=180, stale_max=600)
    txns_5m = int(h.get("txns_5m") or 0)
    ingest_card = _service_card(
        t("health.svc.ingestion"),
        ingest_status,
        t("health.svc.ingestion_val", n=txns_5m),
        t("health.last_seen", ago=_humanize(ingest_age)),
    )

    # 2. Airflow scheduler — alive if any DAG run started recently.
    last_start = dags["last_start"].max() if not dags.empty else None
    sched_age = _age_seconds(last_start)
    sched_status = _status_from_age(sched_age, ok_max=2400, stale_max=7200)
    failed_24h = int(dags["failed_24h"].sum()) if not dags.empty else 0
    if failed_24h > 0 and sched_status == "ok":
        sched_status = "stale"
    sched_card = _service_card(
        t("health.svc.airflow"),
        sched_status,
        t("health.svc.airflow_val", n=len(dags), failed=failed_24h),
        t("health.last_run", ago=_humanize(sched_age)),
    )

    # 3. dbt / Gold layer — freshness from the dbt_transform DAG's last success.
    dbt_age = None
    if not dags.empty:
        dbt_row = dags[dags["dag_id"] == "dbt_transform"]
        if not dbt_row.empty:
            dbt_age = _age_seconds(dbt_row.iloc[0]["last_success"])
    dbt_status = _status_from_age(dbt_age, ok_max=93600, stale_max=180000)  # ~26h / ~50h
    dbt_card = _service_card(
        t("health.svc.dbt"),
        dbt_status,
        t("health.svc.dbt_val"),
        t("health.last_build", ago=_humanize(dbt_age)),
    )

    # 4. Customer onboarding (informational — sparse by design).
    acq_age = _age_seconds(h.get("last_acq"))
    acq_status = _status_from_age(acq_age, ok_max=129600, stale_max=259200)  # 36h / 72h
    acq_card = _service_card(
        t("health.svc.onboarding"),
        acq_status,
        t("health.svc.onboarding_val", n=int(h.get("acquired_today") or 0)),
        t("health.last_acq", ago=_humanize(acq_age)),
    )

    # 5. SAR engine (informational — only fires when alerts exist).
    sar_age = _age_seconds(h.get("last_sar"))
    sar_status = _status_from_age(sar_age, ok_max=604800, stale_max=1209600)  # 7d / 14d
    sar_card = _service_card(
        t("health.svc.sar"),
        sar_status,
        t("health.svc.sar_val", n=int(h.get("sar_today") or 0)),
        t("health.last_sar", ago=_humanize(sar_age)),
    )

    # 6. ML risk layer (ml_score DAG; re-scores active customers every 6h).
    ml_run = fetch_ml_latest_run()
    ml_age = _age_seconds(ml_run.get("trained_at")) if ml_run else None
    ml_status = _status_from_age(ml_age, ok_max=32400, stale_max=129600)  # 9h / 36h
    ml_anom = int(ml_run.get("n_anomalies") or 0) if ml_run else 0
    ml_card = _service_card(
        t("health.svc.ml"),
        ml_status,
        t("health.svc.ml_val", n=ml_anom),
        t("health.last_scored", ago=_humanize(ml_age)),
    )

    cards = [ingest_card, sched_card, dbt_card, acq_card, sar_card, ml_card]
    cols = st.columns(len(cards))
    for col, card in zip(cols, cards):
        col.markdown(card, unsafe_allow_html=True)


def _render_dag_table(dags: pd.DataFrame) -> None:
    st.subheader(t("health.dags_title"))
    if dags.empty:
        st.info(t("health.dags_none"))
        return
    rows = []
    for _, r in dags.iterrows():
        state = (r["last_state"] or "—")
        icon = {"success": "🟢", "failed": "🔴", "running": "🔵", "queued": "🟡"}.get(state, "⚪")
        rows.append(
            {
                t("health.col_dag"): r["dag_id"],
                t("health.col_last_state"): f"{icon} {state}",
                t("health.col_last_run"): _humanize(_age_seconds(r["last_start"])),
                t("health.col_runs_24h"): int(r["runs_24h"]),
                t("health.col_failed_24h"): int(r["failed_24h"]),
                t("health.col_last_success"): _humanize(_age_seconds(r["last_success"])),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _check_row(label: str, ok: bool, detail: str) -> str:
    icon = "✓" if ok else "✗"
    cls = "ok" if ok else "down"
    return (
        f'<div class="hc-check hc-check-{cls}">'
        f'<span class="hc-check-icon">{icon}</span>'
        f'<span class="hc-check-label">{html.escape(label)}</span>'
        f'<span class="hc-check-detail">{html.escape(detail)}</span>'
        f"</div>"
    )


def _render_consistency(checks: dict) -> None:
    st.subheader(t("health.consistency_title"))
    if not checks:
        st.info(t("health.consistency_none"))
        return

    rows = []
    c_live = checks.get("customers_live")
    c_snap = checks.get("customers_snapshot")
    if c_live is not None and c_snap is not None:
        diff = abs(c_live - c_snap)
        ok = diff <= max(5, int(c_live * 0.0001))  # tolerate tiny snapshot lag
        rows.append(
            _check_row(
                t("health.chk_customers"),
                ok,
                t("health.chk_vs", live=f"{c_live:,}", snap=f"{c_snap:,}", diff=diff),
            )
        )
    a_live = checks.get("acquired_today_live")
    a_snap = checks.get("acquired_today_snapshot")
    if a_live is not None and a_snap is not None:
        ok = abs(a_live - a_snap) <= 1
        rows.append(
            _check_row(
                t("health.chk_acquired"),
                ok,
                t("health.chk_vs", live=str(a_live), snap=str(a_snap), diff=abs(a_live - a_snap)),
            )
        )
    if not rows:
        st.info(t("health.consistency_none"))
        return
    st.markdown("".join(rows), unsafe_allow_html=True)
    st.caption(t("health.consistency_caption"))


def _render_throughput(h: dict) -> None:
    st.subheader(t("health.throughput_title"))
    cols = st.columns(5)
    cols[0].metric(t("health.m_txns_today"), f"{int(h.get('txns_today') or 0):,}")
    cols[1].metric(t("health.m_txns_5m"), f"{int(h.get('txns_5m') or 0):,}")
    cols[2].metric(t("health.m_alerts_24h"), f"{int(h.get('alerts_24h') or 0):,}")
    cols[3].metric(t("health.m_customers"), f"{int(h.get('customers_live') or 0):,}")
    cols[4].metric(t("health.m_pg_conns"), int(h.get("pg_conns") or 0))

    by_rule = fetch_alerts_by_rule_24h()
    if not by_rule.empty:
        st.markdown(f"**{t('health.alerts_by_rule_24h')}**")
        st.dataframe(
            by_rule.rename(
                columns={
                    "rule_name": t("alert.col_rule"),
                    "alert_count": t("kpi.alert_count"),
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption(t("health.no_alerts_24h"))


def _render_storage(h: dict) -> None:
    st.subheader(t("health.storage_title"))
    st.metric(t("health.db_size"), _fmt_bytes(h.get("pg_db_size")))
    counts = fetch_table_counts(_HEALTH_TABLES)
    rows = [
        {t("health.col_table"): tbl, t("health.col_rows"): f"{counts.get(tbl, 0):,}"}
        for tbl in _HEALTH_TABLES
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_system_health_page() -> None:
    st.header(t("health.title"))
    top = st.columns([4, 1])
    top[0].caption(t("health.intro"))
    if top[1].button(t("health.refresh"), use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    health = fetch_system_health()
    dags = fetch_airflow_dag_health()
    checks = fetch_consistency_checks()

    st.caption(t("health.as_of", ts=_now().strftime("%Y-%m-%d %H:%M:%S UTC")))

    _render_service_cards(health, dags)
    st.divider()
    _render_dag_table(dags)
    st.divider()
    _render_consistency(checks)
    st.divider()
    _render_throughput(health)
    st.divider()
    _render_storage(health)
