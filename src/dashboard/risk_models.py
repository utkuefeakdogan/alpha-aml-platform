"""Risk Models — the ML risk layer surfaced for compliance analysts.

Reads the latest training run and per-customer scores from Postgres
(aml.ml_model_runs, aml.ml_customer_scores) and renders: model summary KPIs,
the anomaly-score distribution, supervised quality (ROC / PR curves, feature
importances) and the rule-engine vs ML-anomaly overlap. No model is trained
here — that is the `ml_score` Airflow DAG's job; this page is read-only.
"""

from __future__ import annotations

from datetime import datetime, timezone

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.db import (
    fetch_ml_latest_run,
    fetch_ml_overlap,
    fetch_ml_scores,
    fetch_ml_top_anomalies,
)
from src.dashboard.i18n import t

_ACCENT = "#38bdf8"
_DANGER = "#f87171"
_MUTED = "#64748b"


def _humanize_age(ts) -> str:
    if ts is None or pd.isna(ts):
        return t("rm.never")
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    age = int((pd.Timestamp(datetime.now(timezone.utc)) - ts).total_seconds())
    if age < 3600:
        return t("rm.ago_m", n=max(age // 60, 0))
    if age < 86400:
        return t("rm.ago_h", n=age // 3600)
    return t("rm.ago_d", n=age // 86400)


def _render_summary(run: dict) -> None:
    cols = st.columns(6)
    cols[0].metric(t("rm.m_version"), str(run.get("model_version", "—")))
    cols[1].metric(t("rm.m_trained"), _humanize_age(run.get("trained_at")))
    cols[2].metric(t("rm.m_scored"), f"{int(run.get('n_samples') or 0):,}")
    cols[3].metric(t("rm.m_anomalies"), f"{int(run.get('n_anomalies') or 0):,}")
    roc = run.get("roc_auc")
    pr = run.get("pr_auc")
    cols[4].metric(t("rm.m_roc_auc"), f"{float(roc):.3f}" if roc is not None else "—")
    cols[5].metric(t("rm.m_pr_auc"), f"{float(pr):.3f}" if pr is not None else "—")


def _render_distribution(scores: pd.DataFrame) -> None:
    st.subheader(t("rm.dist_title"))
    if scores.empty:
        st.info(t("rm.no_scores"))
        return
    df = scores.copy()
    df["group"] = df["rule_flagged"].map(
        {True: t("rm.legend_rule_flagged"), False: t("rm.legend_not_flagged")}
    )
    chart = (
        alt.Chart(df)
        .mark_bar(opacity=0.8)
        .encode(
            x=alt.X("anomaly_score:Q", bin=alt.Bin(maxbins=40), title=t("rm.anomaly_score")),
            y=alt.Y("count():Q", title=t("rm.customers")),
            color=alt.Color(
                "group:N",
                title=t("rm.legend_title"),
                scale=alt.Scale(
                    domain=[t("rm.legend_rule_flagged"), t("rm.legend_not_flagged")],
                    range=[_DANGER, _ACCENT],
                ),
            ),
            tooltip=[alt.Tooltip("count():Q", title=t("rm.customers"))],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(t("rm.dist_caption"))
    st.caption(t("rm.cap_dist"))


def _render_supervised(run: dict) -> None:
    st.subheader(t("rm.quality_title"))
    if not run.get("supervised_trained"):
        st.info(t("rm.supervised_skipped", reason=run.get("notes") or ""))
        return

    left, right = st.columns(2)
    roc = run.get("roc_curve") or {}
    if roc.get("fpr") and roc.get("tpr"):
        roc_df = pd.DataFrame({"fpr": roc["fpr"], "tpr": roc["tpr"]})
        diag = pd.DataFrame({"fpr": [0, 1], "tpr": [0, 1]})
        roc_line = (
            alt.Chart(roc_df)
            .mark_line(color=_ACCENT)
            .encode(x=alt.X("fpr:Q", title=t("rm.fpr")), y=alt.Y("tpr:Q", title=t("rm.tpr")))
        )
        ref = alt.Chart(diag).mark_line(color=_MUTED, strokeDash=[4, 4]).encode(x="fpr:Q", y="tpr:Q")
        left.markdown(f"**{t('rm.roc_title')}** · AUC = {float(run['roc_auc']):.3f}")
        left.altair_chart((roc_line + ref).properties(height=240), use_container_width=True)
        left.caption(t("rm.cap_roc"))

    pr = run.get("pr_curve") or {}
    if pr.get("recall") and pr.get("precision"):
        pr_df = pd.DataFrame({"recall": pr["recall"], "precision": pr["precision"]})
        pr_line = (
            alt.Chart(pr_df)
            .mark_line(color=_DANGER)
            .encode(
                x=alt.X("recall:Q", title=t("rm.recall")),
                y=alt.Y("precision:Q", title=t("rm.precision")),
            )
        )
        right.markdown(f"**{t('rm.pr_title')}** · AP = {float(run['pr_auc']):.3f}")
        right.altair_chart(pr_line.properties(height=240), use_container_width=True)
        right.caption(t("rm.cap_pr"))

    cols = st.columns(4)
    cols[0].metric(t("rm.precision"), f"{float(run.get('precision_score') or 0):.3f}")
    cols[1].metric(t("rm.recall"), f"{float(run.get('recall_score') or 0):.3f}")
    cols[2].metric(t("rm.f1"), f"{float(run.get('f1_score') or 0):.3f}")
    cols[3].metric(t("rm.positive_rate"), f"{float(run.get('positive_rate') or 0) * 100:.1f}%")

    imp = run.get("feature_importance") or {}
    if imp:
        st.markdown(f"**{t('rm.importance_title')}**")
        imp_df = (
            pd.DataFrame({"feature": list(imp.keys()), "importance": list(imp.values())})
            .sort_values("importance", ascending=False)
        )
        bar = (
            alt.Chart(imp_df)
            .mark_bar(color=_ACCENT)
            .encode(
                x=alt.X("importance:Q", title=t("rm.importance")),
                y=alt.Y("feature:N", sort="-x", title=""),
                tooltip=["feature:N", alt.Tooltip("importance:Q", format=".3f")],
            )
            .properties(height=max(180, 22 * len(imp_df)))
        )
        st.altair_chart(bar, use_container_width=True)
        st.caption(t("rm.cap_importance"))


def _render_overlap(overlap: dict) -> None:
    st.subheader(t("rm.overlap_title"))
    if not overlap or not overlap.get("total"):
        st.info(t("rm.no_scores"))
        return
    cols = st.columns(4)
    cols[0].metric(t("rm.ov_both"), f"{overlap['both']:,}")
    cols[1].metric(t("rm.ov_rule_only"), f"{overlap['rule_only']:,}")
    cols[2].metric(t("rm.ov_ml_only"), f"{overlap['ml_only']:,}")
    cols[3].metric(t("rm.ov_neither"), f"{overlap['neither']:,}")
    st.caption(t("rm.overlap_caption"))
    st.caption(t("rm.cap_overlap"))


def _open_investigation_customer(customer_id: str) -> None:
    st.session_state.nav_target = "Investigation"
    st.session_state.investigation_prefill = customer_id
    st.query_params.clear()
    st.rerun()


def _render_top_anomalies() -> None:
    st.subheader(t("rm.top_title"))
    df = fetch_ml_top_anomalies(limit=20)
    if df.empty:
        st.info(t("rm.no_scores"))
        return

    view = df.rename(
        columns={
            "anomaly_rank": t("rm.col_rank"),
            "customer_id": t("rm.col_customer"),
            "customer_name": t("rm.col_name"),
            "anomaly_score": t("rm.col_anomaly"),
            "triage_score": t("rm.col_triage"),
            "rule_flagged": t("rm.col_rule"),
            "txn_count_30d": t("rm.col_txns"),
            "volume_30d": t("rm.col_volume"),
            "kyc_risk_score": t("rm.col_kyc"),
        }
    )[
        [
            t("rm.col_rank"), t("rm.col_customer"), t("rm.col_name"),
            t("rm.col_anomaly"), t("rm.col_triage"), t("rm.col_rule"),
            t("rm.col_txns"), t("rm.col_volume"), t("rm.col_kyc"),
        ]
    ]
    st.dataframe(view, use_container_width=True, hide_index=True)

    ids = df["customer_id"].tolist()
    pick_cols = st.columns([3, 1])
    target = pick_cols[0].selectbox(t("rm.investigate_label"), ids, key="rm_investigate_pick")
    if pick_cols[1].button(t("rm.investigate_btn"), use_container_width=True):
        _open_investigation_customer(target)


def render_risk_models_page() -> None:
    top = st.columns([4, 1])
    top[0].header(t("rm.title"))
    if top[1].button(t("rm.refresh"), use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(t("rm.intro"))

    with st.expander(t("rm.help_title")):
        st.markdown(t("rm.help_body"))

    run = fetch_ml_latest_run()
    if not run:
        st.info(t("rm.no_run"))
        return

    _render_summary(run)
    st.divider()
    _render_distribution(fetch_ml_scores())
    st.divider()
    _render_supervised(run)
    st.divider()
    _render_overlap(fetch_ml_overlap())
    st.divider()
    _render_top_anomalies()
