"""Reusable UI components."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.i18n import t
from src.common.retention import load_retention
from src.dashboard.db import (
    fetch_alerts_by_rule_total,
    fetch_alerts_24h_trend,
    fetch_alerts_by_rule_24h,
    fetch_dashboard_kpis,
    fetch_live_transactions,
    fetch_live_transactions_count,
    fetch_open_alerts,
    fetch_open_alerts_count,
    fetch_priority_alerts,
    fetch_risk_band_distribution,
    fetch_table_counts,
    fetch_pipeline_metrics,
)


def hash_id(value: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        return "—"
    return hashlib.sha256(str(value).encode()).hexdigest()[:16]


def _mask_identity(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    s = str(val)
    if len(s) <= 4:
        return "****"
    return s[:3] + "****" + s[-2:]


def render_kyc_kpi_row(on_nav) -> None:
    """AML/KYC KPI cards with navigation shortcuts."""
    try:
        kpis = fetch_dashboard_kpis()
    except Exception as exc:
        st.warning(t("kpi.unavailable", error=str(exc)))
        return

    cards = [
        (t("kpi.total_customers"), kpis["customers"], t("kpi.total_customers_hint"), "Investigation"),
        (t("kpi.alert_count"), kpis["total_alerts"], t("kpi.alert_count_hint"), "SAR Archive"),
        (t("kpi.active_scenarios"), kpis["scenarios"], t("kpi.active_scenarios_hint"), "Rule Builder"),
    ]
    cols = st.columns(3)
    for col, (label, value, hint, target) in zip(cols, cards):
        with col:
            st.markdown(
                f'<div class="aml-metric-card"><div class="aml-metric-label">{label}</div>'
                f'<div class="aml-metric-value">{value:,}</div>'
                f'<div class="aml-metric-delta">{hint}</div></div>',
                unsafe_allow_html=True,
            )
            if st.button(t("common.view"), key=f"kpi_nav_{target}", use_container_width=True):
                on_nav(target)


def render_risk_pie_chart(*, embedded: bool = False, chart_height: int = 280) -> None:
    """Customer KYC risk band distribution."""
    if not embedded:
        st.subheader(t("risk.title"))
    bands = fetch_risk_band_distribution()
    if not bands.empty:
        bands = bands.copy()
        bands["risk_band"] = bands["risk_band"].map(lambda k: t(f"risk_band.{k}"))
    if bands.empty:
        st.info(t("risk.no_data"))
        return
    chart = (
        alt.Chart(bands)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("customer_count:Q"),
            color=alt.Color("risk_band:N", title="Risk Band"),
            tooltip=["risk_band", "customer_count"],
        )
        .properties(height=chart_height)
    )
    st.altair_chart(chart, use_container_width=True)
    if not embedded:
        st.caption(t("risk.caption"))


def render_metric_cards() -> dict[str, int] | None:
    """Deprecated — use render_kyc_kpi_row."""
    render_kyc_kpi_row(lambda _: None)
    return None


def _relative_time(ts) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    delta = pd.Timestamp.now(tz="UTC") - t
    mins = max(0, int(delta.total_seconds() // 60))
    if mins < 60:
        return f"{mins}m ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def _alert_route(row: pd.Series) -> str:
    sc = row.get("sender_country") or "—"
    rc = row.get("receiver_country") or row.get("country_code") or "—"
    if pd.isna(sc):
        sc = "—"
    if pd.isna(rc):
        rc = "—"
    return f"{sc} → {rc}"


def format_alerts_table(df: pd.DataFrame) -> pd.DataFrame:
    """Compact summary table (no duplicate columns)."""
    if df.empty:
        return df
    out = pd.DataFrame()
    out["Rule"] = df["rule_name"]
    out["Window"] = df["window_type"].fillna("—")
    out["Amount"] = df.apply(
        lambda r: f"{float(r['amount']):,.0f} {r['currency']}" if pd.notna(r.get("amount")) else "—",
        axis=1,
    )
    out["EUR"] = df["amount_eur"].apply(
        lambda x: f"€{float(x):,.0f}" if pd.notna(x) else "—"
    )
    out["Route"] = df.apply(_alert_route, axis=1)
    out["Sender"] = df["sender_name"].fillna("External")
    out["Receiver"] = df["receiver_name"].fillna("—")
    out["Flagged"] = df["flagged_at"].apply(_relative_time)
    out["Detail"] = df["rule_detail"].fillna("").str.slice(0, 60)
    return out


def render_open_alerts_table(
    alerts: pd.DataFrame,
    on_investigate,
    key_prefix: str = "alert",
) -> None:
    """Unified open-alerts list with Alert ID and Investigate action."""
    if alerts.empty:
        st.info(t("alert.no_open"))
        return

    header = st.columns([3, 2, 1, 2, 2, 1])
    for col, label in zip(
        header,
        [t("alert.col_id"), t("alert.col_customer"), t("alert.col_priority"), t("alert.col_rule"), t("alert.col_time"), ""],
    ):
        col.markdown(f"**{label}**")

    for idx, row in alerts.iterrows():
        alert_id = str(row["alert_id"])
        rule = str(row["rule_name"])
        cols = st.columns([3, 2, 1, 2, 2, 1])
        cols[0].markdown(f"`{alert_id}`")
        cols[1].write(str(row.get("customer_name") or "—"))
        priority = row.get("alert_priority_score")
        cols[2].write(f"{float(priority):.1f}" if pd.notna(priority) else "—")
        cols[3].write(rule)
        cols[4].write(_relative_time(row.get("flagged_at")))
        if cols[5].button(
            t("common.investigate"),
            key=f"{key_prefix}_inv_{alert_id}_{rule}_{idx}",
            use_container_width=True,
        ):
            on_investigate(alert_id, rule)


def render_live_recent_alerts(on_investigate, limit: int = 5) -> None:
    """Top N alerts, auto-refreshed every 10 seconds."""
    st.caption(t("monitoring.live_refresh"))

    def _draw() -> None:
        render_open_alerts_table(fetch_priority_alerts(limit), on_investigate, key_prefix="recent")

    if hasattr(st, "fragment"):
        @st.fragment(run_every=10)
        def _poll() -> None:
            _draw()

        _poll()
    else:
        _draw()


def _render_transactions_scroll_table(txns: pd.DataFrame) -> None:
    """Full transaction details in a horizontally scrollable table."""
    if txns.empty:
        st.info(t("monitoring.no_transactions"))
        return
    formatted = format_transactions_table(txns)
    row_h = 36
    st.dataframe(
        formatted,
        use_container_width=False,
        hide_index=True,
        height=min(420, row_h * len(formatted) + 38),
        width="content",
    )


def render_paginated_live_transactions_feed(_on_customer, page_size: int = 10) -> None:
    """Live transactions — 10 per page, full columns, horizontal scroll."""
    if "live_txn_offset" not in st.session_state:
        st.session_state.live_txn_offset = 0

    def _draw() -> None:
        offset = int(st.session_state.live_txn_offset)
        total = fetch_live_transactions_count()
        page = fetch_live_transactions(limit=page_size, offset=offset)
        start = offset + 1 if total else 0
        end = min(offset + page_size, total)
        st.caption(t("monitoring.showing_txns", start=start, end=end, total=total))
        _render_transactions_scroll_table(page)
        nav1, _, nav3 = st.columns([1, 2, 1])
        with nav1:
            if st.button(t("common.previous"), disabled=offset == 0, key="txn_prev"):
                st.session_state.live_txn_offset = max(0, offset - page_size)
                st.rerun()
        with nav3:
            if st.button(t("common.next"), disabled=offset + page_size >= total, key="txn_next"):
                st.session_state.live_txn_offset = offset + page_size
                st.rerun()

    st.caption(t("monitoring.live_refresh"))
    if hasattr(st, "fragment"):
        @st.fragment(run_every=10)
        def _poll() -> None:
            _draw()

        _poll()
    else:
        _draw()


def render_live_transactions_feed(on_customer, limit: int = 150) -> None:
    """Deprecated — use render_paginated_live_transactions_feed."""
    render_paginated_live_transactions_feed(on_customer, page_size=10)


def render_paginated_open_alerts(on_investigate, page_size: int = 10) -> None:
    """All open alerts, 10 per page with navigation."""
    if "open_alerts_offset" not in st.session_state:
        st.session_state.open_alerts_offset = 0

    offset = int(st.session_state.open_alerts_offset)
    total = fetch_open_alerts_count()
    page = fetch_open_alerts(limit=page_size, offset=offset)

    start = offset + 1 if total else 0
    end = min(offset + page_size, total)
    st.caption(t("monitoring.showing_alerts", start=start, end=end, total=total))

    render_open_alerts_table(page, on_investigate, key_prefix="all")

    nav1, nav2, nav3 = st.columns([1, 2, 1])
    with nav1:
        if st.button(t("common.previous"), disabled=offset == 0, key="alerts_prev"):
            st.session_state.open_alerts_offset = max(0, offset - page_size)
            st.rerun()
    with nav3:
        if st.button(t("common.next"), disabled=offset + page_size >= total, key="alerts_next"):
            st.session_state.open_alerts_offset = offset + page_size
            st.rerun()


def render_priority_alerts_table(alerts: pd.DataFrame, on_investigate) -> None:
    """Deprecated — use render_open_alerts_table."""
    render_open_alerts_table(alerts, on_investigate, key_prefix="pri")


def render_recent_alerts(
    alerts: pd.DataFrame,
    on_select,
    selected: dict | None = None,
    max_rows: int = 15,
) -> None:
    """Card-based recent alerts with Investigate action on the right."""
    if alerts.empty:
        st.info("No alerts in the last period.")
        return

    for idx, (_, row) in enumerate(alerts.head(max_rows).iterrows()):
        rule = str(row.get("rule_name", ""))
        window = str(row.get("window_type") or "")
        txn_id = str(row["txn_id"])
        key_suffix = f"{txn_id}_{rule}_{idx}"
        is_active = (
            selected
            and selected.get("txn_id") == txn_id
            and selected.get("rule_name") == rule
        )

        amt_local = (
            f"{float(row['amount']):,.0f} {row.get('currency', 'EUR')}"
            if pd.notna(row.get("amount"))
            else "—"
        )
        amt_eur_val = row.get("amount_eur") or row.get("amount")
        amt_eur = f"€{float(amt_eur_val):,.0f}" if pd.notna(amt_eur_val) else "—"
        sender = row.get("sender_name") or "External"
        receiver = row.get("receiver_name") or "—"
        route = _alert_route(row)
        flagged = _relative_time(row.get("flagged_at"))
        detail = str(row.get("rule_detail") or "")[:90]
        window_tag = f" · {window}" if window and window != "—" else ""
        detail_html = f'<div class="aml-alert-detail">{detail}</div>' if detail else ""
        border = "#38bdf8" if is_active else "#334155"

        c_card, c_btn = st.columns([11, 1], gap="small")
        with c_card:
            st.markdown(
                f'<div class="aml-alert-card" style="border-left-color:{border}">'
                f'<div class="aml-alert-meta"><span class="aml-alert-rule">{rule}</span>{window_tag}'
                f' &nbsp;·&nbsp; {flagged}</div>'
                f'<div class="aml-alert-amount">{amt_local} <span style="color:#94a3b8;font-weight:400">({amt_eur})</span>'
                f' &nbsp;·&nbsp; {route}</div>'
                f'<div class="aml-alert-party">{sender} → {receiver}</div>'
                f'{detail_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
        with c_btn:
            st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)
            if st.button(
                t("common.investigate"),
                key=f"inv_{key_suffix}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                on_select(txn_id, rule)

    with st.expander("Alert log (table view)", expanded=False):
        st.dataframe(format_alerts_table(alerts.head(max_rows)), use_container_width=True, hide_index=True)


def render_investigation_workspace(
    alert_row: pd.Series,
    customer_id: str,
    on_generate_sar,
    on_dismiss,
    notes_key: str = "inv_notes",
) -> None:
    """Expanded investigation view below alert list."""
    rule = str(alert_row["rule_name"])
    txn_id = str(alert_row["txn_id"])
    st.markdown(
        '<div class="aml-investigation-panel">'
        '<div class="aml-investigation-title">Investigation Workspace</div></div>',
        unsafe_allow_html=True,
    )

    from src.dashboard.db import fetch_customer_profile, fetch_customer_transactions

    profile = fetch_customer_profile(customer_id)
    amt = float(alert_row.get("amount_eur") or alert_row.get("amount") or 0)
    cur = alert_row.get("currency", "EUR")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rule", rule)
    m2.metric("Alert Amount", f"{float(alert_row.get('amount', 0)):,.0f} {cur}")
    m3.metric("EUR Equivalent", f"€{amt:,.0f}")
    m4.metric("Customer", customer_id)

    if profile:
        c = profile["customer"]
        st.caption(
            f"{c['name']} · Branch {c['branch_id']} · "
            f"Risk {float(c['risk_score']):.1f} · Segment {c['segment']} · "
            f"PEP: {'Yes' if c['is_pep'] else 'No'}"
        )
        s1, s2, s3 = st.columns(3)
        s1.metric("30d Volume (EUR)", f"{float(profile['spend']['volume_30d']):,.0f}")
        s2.metric("30d Txns", int(profile["spend"]["txn_count_30d"]))
        s3.metric("Rule Detail", str(alert_row.get("rule_detail", ""))[:40])

    st.markdown("**Customer transactions**")
    txns = fetch_customer_transactions(customer_id, limit=80)
    st.dataframe(format_transactions_table(txns), use_container_width=True, hide_index=True)

    st.markdown("**Analyst actions**")
    notes = st.text_area(
        "Investigation notes",
        value=st.session_state.get(notes_key, ""),
        placeholder="Document findings, rationale for dismissal, or SAR escalation context…",
        height=100,
        key=f"{notes_key}_input",
    )
    st.session_state[notes_key] = notes

    a1, a2, a3 = st.columns([2, 2, 6])
    with a1:
        if st.button("Generate SAR", type="primary", use_container_width=True):
            on_generate_sar(txn_id, rule, notes)
    with a2:
        if st.button("Flag as non-alert", use_container_width=True):
            on_dismiss(txn_id, rule, notes)
    with a3:
        if st.button("Close investigation", use_container_width=True):
            st.session_state.selected_alert = None
            st.rerun()


def render_alert_action_rows(alerts: pd.DataFrame, on_investigate, on_sar, max_rows: int = 12) -> None:
    """Deprecated — use render_recent_alerts."""
    render_recent_alerts(alerts, on_investigate, max_rows=max_rows)


def format_transactions_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("sender_branch", "receiver_branch", "sender_country", "receiver_country",
                "sender_customer_no", "receiver_customer_no"):
        if col in out.columns:
            out[col] = out[col].fillna("—")
    if "sender_identity_no" in out.columns:
        out["Sender ID No"] = out["sender_identity_no"].map(_mask_identity)
    if "receiver_identity_no" in out.columns:
        out["Receiver ID No"] = out["receiver_identity_no"].map(_mask_identity)
    if "amount_eur" in out.columns:
        out["Amount (EUR)"] = out["amount_eur"].apply(lambda x: f"{float(x):,.2f}" if pd.notna(x) else "—")
    out["Trx Amount"] = out.apply(
        lambda r: f"{float(r['amount']):,.2f} {r['currency']}" if pd.notna(r.get("amount")) else "—",
        axis=1,
    )
    out = out.rename(
        columns={
            "txn_id": "Transaction ID",
            "txn_category": "Category",
            "txn_type": "Type",
            "sender_name": "Sender Name",
            "receiver_name": "Receiver Name",
            "sender_customer_no": "Sender Cust No",
            "receiver_customer_no": "Receiver Cust No",
            "sender_branch": "Sender Branch",
            "receiver_branch": "Receiver Branch",
            "sender_country": "Sender Country",
            "receiver_country": "Receiver Country",
            "txn_description": "Description",
            "country_code": "Country",
            "ts": "Timestamp",
            "ingested_at": "Ingested At",
            "branch_id": "Branch",
        }
    )
    cols = [
        "Transaction ID",
        "Sender Cust No", "Sender Name", "Sender ID No", "Sender Branch", "Sender Country",
        "Receiver Cust No", "Receiver Name", "Receiver ID No", "Receiver Branch", "Receiver Country",
        "Category", "Type", "Trx Amount", "Amount (EUR)", "Description", "Country",
        "Timestamp", "Ingested At", "Branch",
    ]
    return out[[c for c in cols if c in out.columns]]




def render_alerts_by_rule_bar(*, compact: bool = False, chart_height: int = 280) -> None:
    """Bar chart of alert counts by rule (24h)."""
    if not compact:
        st.subheader(t("monitoring.alerts_by_rule"))
    summary = fetch_alerts_by_rule_24h()
    if summary.empty:
        st.info(t("monitoring.no_alerts_24h"))
        return
    chart = (
        alt.Chart(summary)
        .mark_bar(color="#38bdf8")
        .encode(
            x=alt.X("alert_count:Q", title="Count (24h)"),
            y=alt.Y("rule_name:N", sort="-x", title="Rule"),
            tooltip=["rule_name", "alert_count"],
        )
        .properties(height=chart_height)
    )
    st.altair_chart(chart, use_container_width=True)


def render_alerts_trend_chart() -> None:
    st.subheader("Alerts by Rule (24h Trend)")
    summary = fetch_alerts_by_rule_24h()
    if summary.empty:
        st.info("No alerts in the last 24 hours.")
        return
    chart = (
        alt.Chart(summary)
        .mark_bar(color="#38bdf8")
        .encode(
            x=alt.X("alert_count:Q", title="Count (24h)"),
            y=alt.Y("rule_name:N", sort="-x", title="Rule"),
            tooltip=["rule_name", "alert_count"],
        )
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)
    trend = fetch_alerts_24h_trend()
    if not trend.empty:
        line = (
            alt.Chart(trend)
            .mark_line(point=True)
            .encode(x="hour_bucket:T", y="alert_count:Q", color="rule_name:N")
            .properties(height=180)
        )
        st.altair_chart(line, use_container_width=True)


def render_data_pipeline_panel() -> None:
    """Pipeline snapshot — alert counts use the same live KPIs as Overview / Monitoring."""
    with st.expander(t("pipeline.title"), expanded=False):
        try:
            kpis = fetch_dashboard_kpis()
            st.metric(t("kpi.alert_count"), kpis["total_alerts"])
            st.caption(t("kpi.alert_count_hint"))
        except Exception as exc:
            st.warning(t("kpi.unavailable", error=str(exc)))

        by_rule = fetch_alerts_by_rule_total()
        if not by_rule.empty:
            st.markdown(f"**{t('pipeline.alerts_by_rule')}**")
            st.dataframe(
                by_rule.rename(columns={"rule_name": t("alert.col_rule"), "alert_count": t("kpi.alert_count")}),
                use_container_width=True,
                hide_index=True,
            )

        metrics = fetch_pipeline_metrics(limit=30)
        if metrics.empty:
            st.caption(t("pipeline.hint"))
            return

        latest = metrics.drop_duplicates(subset=["metric_key"], keep="first")
        cust = latest[latest["metric_key"].isin(["customers_total", "customers_active", "customers_acquired_today"])]
        if not cust.empty:
            st.markdown(f"**{t('pipeline.customer_stats')}**")
            cols = st.columns(3)
            for col, key in zip(
                cols,
                ["customers_total", "customers_active", "customers_acquired_today"],
            ):
                row = cust[cust["metric_key"] == key]
                if not row.empty:
                    col.metric(t(f"pipeline.{key}"), int(row.iloc[0]["metric_value"]))
        st.caption(t("pipeline.airflow_dag"))


def render_retention_panel() -> None:
    """Data retention policy overview (source: configs/retention.json)."""
    with st.expander(t("retention.title"), expanded=False):
        cfg = load_retention()
        policies = cfg.get("policies", {})
        if not policies:
            st.info(t("retention.no_data"))
            return

        tables = [p.get("table") for p in policies.values() if p.get("table")]
        counts = fetch_table_counts(tuple(tables))

        rows = []
        for pol in policies.values():
            table = pol.get("table", "")
            rows.append(
                {
                    t("retention.col_data"): pol.get("label", table),
                    t("retention.col_table"): table,
                    t("retention.col_retention"): f"{pol.get('value', '?')} {pol.get('unit', '')}".strip(),
                    t("retention.col_rows"): f"{counts.get(table, 0):,}",
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        guards = cfg.get("guards", {})
        if guards:
            cols = st.columns(4)
            cols[0].metric(t("retention.disk_warn"), f"{guards.get('disk_warn_pct', '—')}%")
            cols[1].metric(t("retention.disk_critical"), f"{guards.get('disk_critical_pct', '—')}%")
            cols[2].metric(t("retention.pg_warn"), f"{guards.get('pg_db_warn_gb', '—')} GB")
            cols[3].metric(t("retention.metrics_emergency"), f"{guards.get('metrics_emergency_max_rows', 0):,}")

        for pol in policies.values():
            desc = pol.get("description")
            if desc:
                st.caption(f"**{pol.get('label', pol.get('table', ''))}** — {desc}")
        st.caption(t("retention.source_hint"))


def render_airflow_footer() -> None:
    nxt = datetime.utcnow().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    st.markdown(
        f'<div class="aml-footer"><strong>{t("footer.lifecycle")}</strong> — '
        f"{t('footer.raw')} "
        f'<span class="aml-badge-ok">{nxt:%Y-%m-%d %H:%M}</span></div>',
        unsafe_allow_html=True,
    )
