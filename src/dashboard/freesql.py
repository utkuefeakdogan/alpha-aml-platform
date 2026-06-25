"""SQL Explorer — IDE-style read-only query workspace."""

from __future__ import annotations

import html
import time
import urllib.parse

import pandas as pd
import streamlit as st

from src.dashboard.db import (
    FREESQL_CONTACT_EMAIL,
    FREESQL_USER,
    fetch_freesql_allowed_tables,
    fetch_freesql_schema,
    run_sql_readonly,
)
from src.dashboard.i18n import t

# Example queries grouped by category (SQL is English; labels come from i18n).
_EXAMPLE_CATEGORIES: dict[str, list[tuple[str, str]]] = {
    "alerts": [
        (
            "sql.ex.alerts.by_rule",
            "SELECT rule_name, COUNT(*) AS alerts\n"
            "FROM aml.flagged_transactions\n"
            "WHERE flagged_at >= NOW() - INTERVAL '24 hours'\n"
            "GROUP BY rule_name ORDER BY alerts DESC",
        ),
        (
            "sql.ex.alerts.open",
            "SELECT f.txn_id, f.customer_id, f.rule_name, f.flagged_at, f.alert_priority_score\n"
            "FROM aml.flagged_transactions f\n"
            "LEFT JOIN aml.alert_dispositions d\n"
            "  ON d.txn_id = f.txn_id AND d.rule_name = f.rule_name\n"
            "WHERE d.id IS NULL\n"
            "ORDER BY f.alert_priority_score DESC NULLS LAST\n"
            "LIMIT 25",
        ),
        (
            "sql.ex.alerts.priority",
            "SELECT rule_name, ROUND(AVG(alert_priority_score), 1) AS avg_priority,\n"
            "       COUNT(*) AS cnt\n"
            "FROM aml.flagged_transactions\n"
            "GROUP BY rule_name ORDER BY avg_priority DESC",
        ),
        (
            "sql.ex.alerts.trend",
            "SELECT date_trunc('hour', flagged_at) AS hour, COUNT(*) AS alerts\n"
            "FROM aml.flagged_transactions\n"
            "WHERE flagged_at >= NOW() - INTERVAL '7 days'\n"
            "GROUP BY 1 ORDER BY 1 DESC",
        ),
        (
            "sql.ex.alerts.archive",
            "SELECT alert_txn_id, rule_name, customer_id,\n"
            "       COUNT(*) AS archived_txns, MIN(archived_at) AS first_archived\n"
            "FROM aml.alert_transaction_archive\n"
            "GROUP BY 1, 2, 3 ORDER BY first_archived DESC LIMIT 20",
        ),
    ],
    "transactions": [
        (
            "sql.ex.txn.latest",
            "SELECT txn_id, sender_customer_no, receiver_customer_no,\n"
            "       amount_eur, currency, ingested_at\n"
            "FROM aml.transactions ORDER BY ingested_at DESC LIMIT 20",
        ),
        (
            "sql.ex.txn.high_value",
            "SELECT txn_id, sender_customer_no, amount_eur, currency, ts\n"
            "FROM aml.transactions\n"
            "WHERE amount_eur >= 5000\n"
            "ORDER BY amount_eur DESC LIMIT 15",
        ),
        (
            "sql.ex.txn.by_country",
            "SELECT sender_country, COUNT(*) AS txns, ROUND(SUM(amount_eur), 2) AS volume_eur\n"
            "FROM aml.transactions\n"
            "WHERE sender_country IS NOT NULL\n"
            "GROUP BY sender_country ORDER BY volume_eur DESC LIMIT 10",
        ),
    ],
    "customers": [
        (
            "sql.ex.cust.risk_bands",
            "SELECT CASE WHEN risk_score >= 70 THEN 'high'\n"
            "            WHEN risk_score >= 40 THEN 'medium' ELSE 'low' END AS band,\n"
            "       COUNT(*) AS customers\n"
            "FROM aml.customers GROUP BY band ORDER BY customers DESC",
        ),
        (
            "sql.ex.cust.pep",
            "SELECT customer_id, name, risk_score, segment, country\n"
            "FROM aml.customers WHERE is_pep = true ORDER BY risk_score DESC LIMIT 20",
        ),
        (
            "sql.ex.cust.addresses",
            "SELECT customer_id, COUNT(*) AS address_count\n"
            "FROM aml.customer_addresses\n"
            "GROUP BY customer_id ORDER BY address_count DESC LIMIT 15",
        ),
        (
            "sql.ex.cust.acquired",
            "SELECT DATE(acquired_at) AS day, COUNT(*) AS new_customers\n"
            "FROM aml.customer_acquisition_log\n"
            "GROUP BY 1 ORDER BY 1 DESC LIMIT 14",
        ),
    ],
    "sar": [
        (
            "sql.ex.sar.recent",
            "SELECT report_id, customer_id, flagged_count, model_used, created_at\n"
            "FROM aml.sar_reports ORDER BY created_at DESC LIMIT 15",
        ),
        (
            "sql.ex.sar.dispositions",
            "SELECT disposition, COUNT(*) AS cnt\n"
            "FROM aml.alert_dispositions\n"
            "GROUP BY disposition ORDER BY cnt DESC",
        ),
        (
            "sql.ex.sar.open_vs_closed",
            "SELECT COUNT(*) FILTER (WHERE d.id IS NULL) AS open_alerts,\n"
            "       COUNT(*) FILTER (WHERE d.id IS NOT NULL) AS dispositioned\n"
            "FROM aml.flagged_transactions f\n"
            "LEFT JOIN aml.alert_dispositions d\n"
            "  ON d.txn_id = f.txn_id AND d.rule_name = f.rule_name",
        ),
    ],
    "pipeline": [
        (
            "sql.ex.pipe.metrics",
            "SELECT metric_key, metric_value, recorded_at\n"
            "FROM aml.pipeline_metrics\n"
            "ORDER BY recorded_at DESC LIMIT 20",
        ),
        (
            "sql.ex.pipe.window_metrics",
            "SELECT window_type, COUNT(*) AS rows,\n"
            "       MAX(computed_at) AS latest\n"
            "FROM aml.account_window_metrics\n"
            "GROUP BY window_type",
        ),
        (
            "sql.ex.pipe.behavior",
            "SELECT customer_id, txn_count_30d, volume_30d, last_txn_at\n"
            "FROM aml.customer_behavior_30d\n"
            "ORDER BY volume_30d DESC NULLS LAST LIMIT 15",
        ),
    ],
    "ml": [
        (
            "sql.ex.ml.top_anomalies",
            "SELECT customer_id, anomaly_score, anomaly_rank,\n"
            "       triage_score, rule_flagged\n"
            "FROM aml.ml_customer_scores\n"
            "ORDER BY anomaly_score DESC LIMIT 20",
        ),
        (
            "sql.ex.ml.ml_only",
            "SELECT customer_id, anomaly_score, triage_score,\n"
            "       txn_count_30d, volume_30d\n"
            "FROM aml.ml_customer_scores\n"
            "WHERE is_anomaly = true AND rule_flagged = false\n"
            "ORDER BY anomaly_score DESC LIMIT 20",
        ),
        (
            "sql.ex.ml.overlap",
            "SELECT rule_flagged, is_anomaly, COUNT(*) AS customers\n"
            "FROM aml.ml_customer_scores\n"
            "GROUP BY rule_flagged, is_anomaly ORDER BY customers DESC",
        ),
        (
            "sql.ex.ml.runs",
            "SELECT model_version, trained_at, n_samples, n_anomalies,\n"
            "       roc_auc, pr_auc, supervised_trained\n"
            "FROM aml.ml_model_runs ORDER BY trained_at DESC LIMIT 10",
        ),
    ],
}

_CATEGORY_ORDER = ["alerts", "transactions", "customers", "sar", "pipeline", "ml"]

_DEFAULT_SQL = (
    "SELECT rule_name, COUNT(*) AS alerts\n"
    "FROM aml.flagged_transactions\n"
    "GROUP BY rule_name"
)


def _load_sql(query: str) -> None:
    """Load query into the editor on the next run (before text_area is built)."""
    st.session_state["_sql_text_override"] = query
    st.rerun()


def _render_security_banner() -> None:
    email = FREESQL_CONTACT_EMAIL
    subject = urllib.parse.quote(t("sql.mail.subject"))
    mailto = f"mailto:{email}?subject={subject}"
    body = t("sql.security.body", user=FREESQL_USER, email=email).replace(
        email, f'<a href="{mailto}">{html.escape(email)}</a>', 1
    )
    st.markdown(
        f"""
        <div class="sql-ide-banner">
            <div class="sql-ide-banner-title">{t("sql.security.title")}</div>
            <div class="sql-ide-banner-body">
                {body}
                <br><br>
                <span class="sql-ide-user-badge">{html.escape(FREESQL_USER)}</span>
                &nbsp;·&nbsp; {t("sql.security.readonly")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_schema_browser() -> None:
    st.markdown(
        f"<div class='sql-ide-panel-title'>{t('sql.schema_title')}</div>",
        unsafe_allow_html=True,
    )
    try:
        schema = fetch_freesql_schema()
        tables = fetch_freesql_allowed_tables()
    except Exception as exc:
        st.error(str(exc))
        return
    if not tables:
        st.info(t("sql.schema_empty"))
        return

    st.markdown(
        f"<span class='sql-ide-schema-badge'>{t('sql.schema_count').format(n=len(tables))}</span>",
        unsafe_allow_html=True,
    )
    picked = st.selectbox(
        t("sql.pick_table"),
        tables,
        format_func=lambda name: f"aml.{name}",
        key="sql_schema_pick",
        label_visibility="collapsed",
    )
    cols = schema[schema["table_name"] == picked][["column_name", "data_type"]]
    st.caption(t("sql.column_count").format(n=len(cols)))
    st.dataframe(cols, use_container_width=True, hide_index=True, height=140)
    if st.button(t("sql.insert_table"), key=f"sql_tbl_{picked}", use_container_width=True):
        _load_sql(f"SELECT *\nFROM aml.{picked}\nLIMIT 50")


def _render_examples() -> None:
    st.markdown("<div class='sql-ide-examples-wrap'>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='sql-ide-panel-title'>{t('sql.examples')}</div>",
        unsafe_allow_html=True,
    )
    tabs = st.tabs([t(f"sql.cat.{cat}") for cat in _CATEGORY_ORDER])
    for tab, cat_key in zip(tabs, _CATEGORY_ORDER):
        with tab:
            items = _EXAMPLE_CATEGORIES.get(cat_key, [])
            for i in range(0, len(items), 2):
                c1, c2 = st.columns(2, gap="small")
                pair = items[i : i + 2]
                for col, (label_key, query) in zip((c1, c2), pair):
                    with col:
                        if st.button(
                            t(label_key),
                            key=f"ex_{cat_key}_{label_key}",
                            use_container_width=True,
                        ):
                            _load_sql(query)
    st.markdown("</div>", unsafe_allow_html=True)


def render_freesql_page() -> None:
    st.header(t("sql.title"))
    st.caption(t("sql.subtitle"))
    _render_security_banner()

    if "sql_text" not in st.session_state:
        st.session_state.sql_text = _DEFAULT_SQL
    if "_sql_text_override" in st.session_state:
        st.session_state.sql_text = st.session_state.pop("_sql_text_override")

    _render_examples()

    schema_col, editor_col = st.columns([0.72, 3], gap="small")
    with schema_col:
        st.markdown("<div class='sql-ide-panel sql-ide-schema-panel'>", unsafe_allow_html=True)
        _render_schema_browser()
        st.markdown("</div>", unsafe_allow_html=True)

    with editor_col:
        st.markdown(f"**{t('sql.editor_title')}**")
        sql_text = st.text_area(
            t("sql.query_label"),
            height=200,
            key="sql_text",
            label_visibility="collapsed",
            placeholder="SELECT ...",
        )
        c1, c2, c3 = st.columns([1, 1, 2])
        max_rows = c1.number_input(t("sql.max_rows"), min_value=10, max_value=5000, value=500, step=10)
        timeout_s = c2.number_input(t("sql.timeout"), min_value=1, max_value=30, value=8, step=1)
        run = c3.button(t("sql.run"), type="primary", use_container_width=True)

    st.caption(t("sql.safety_note"))

    if run:
        try:
            start = time.perf_counter()
            df = run_sql_readonly(sql_text, max_rows=int(max_rows), timeout_ms=int(timeout_s) * 1000)
            elapsed = (time.perf_counter() - start) * 1000
            st.markdown("---")
            st.markdown(f"**{t('sql.executed_query')}**")
            st.code(sql_text.strip(), language="sql")
            st.success(t("sql.result_meta").format(rows=len(df), ms=f"{elapsed:.0f}"))
            if df.empty:
                st.info(t("sql.no_rows"))
            else:
                st.markdown(f"**{t('sql.results_title')}**")
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    t("sql.download_csv"),
                    df.to_csv(index=False).encode("utf-8"),
                    file_name="query_result.csv",
                    mime="text/csv",
                )
        except Exception as exc:
            st.error(f"{t('sql.error')}: {exc}")

    st.markdown("---")
    email = FREESQL_CONTACT_EMAIL
    subject = urllib.parse.quote(t("sql.mail.subject"))
    st.markdown(
        t("sql.contact_line").format(
            email=f"[{email}](mailto:{email}?subject={subject})"
        ),
        unsafe_allow_html=True,
    )
