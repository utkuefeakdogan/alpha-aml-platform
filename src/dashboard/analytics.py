"""Analytics — a data-analyst view over the AML platform.

Six read-only sections (transaction trend, alert trend & rate, customer
acquisition, segmentation, cross-border corridors, risk analytics). Each
section is wrapped in try/except so one failing query never blanks the page;
aggregates are cached in db.py. Charts deliberately do NOT duplicate the live
Monitoring/Risk Models charts — this page is about trends and breakdowns.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.db import (
    fetch_acquisition_trend,
    fetch_alert_trend,
    fetch_geo_corridors,
    fetch_risk_analytics,
    fetch_segmentation,
    fetch_txn_trend,
)
from src.dashboard.i18n import t

_ACCENT = "#38bdf8"
_DANGER = "#f87171"
_GOOD = "#34d399"
_MUTED = "#94a3b8"


def _empty(msg_key: str = "analytics.no_data") -> None:
    st.info(t(msg_key))


def _section_txn_trend(days: int) -> None:
    st.subheader(t("analytics.s1_title"))
    df = fetch_txn_trend(days)
    if df.empty:
        _empty()
        return
    df = df.copy()
    df["day"] = pd.to_datetime(df["day"])
    base = alt.Chart(df).encode(x=alt.X("day:T", title=t("analytics.day")))
    bars = base.mark_bar(color=_ACCENT, opacity=0.35).encode(
        y=alt.Y("txn_count:Q", title=t("analytics.txn_count")),
        tooltip=["day:T", alt.Tooltip("txn_count:Q", title=t("analytics.txn_count"))],
    )
    line = base.mark_line(color=_DANGER, strokeWidth=2).encode(
        y=alt.Y("volume_eur:Q", title=t("analytics.volume"), axis=alt.Axis(titleColor=_DANGER)),
        tooltip=["day:T", alt.Tooltip("volume_eur:Q", title=t("analytics.volume"), format=",.0f")],
    )
    chart = alt.layer(bars, line).resolve_scale(y="independent").properties(height=280)
    st.altair_chart(chart, use_container_width=True)
    st.caption(t("analytics.s1_insight"))


def _section_alert_trend(days: int) -> None:
    st.subheader(t("analytics.s2_title"))
    df = fetch_alert_trend(days)
    if df.empty:
        _empty()
        return
    df = df.copy()
    df["day"] = pd.to_datetime(df["day"])
    base = alt.Chart(df).encode(x=alt.X("day:T", title=t("analytics.day")))
    bars = base.mark_bar(color=_DANGER, opacity=0.4).encode(
        y=alt.Y("alerts:Q", title=t("analytics.alerts")),
        tooltip=["day:T", alt.Tooltip("alerts:Q", title=t("analytics.alerts"))],
    )
    line = base.mark_line(color=_ACCENT, strokeWidth=2).encode(
        y=alt.Y("alerts_per_1k:Q", title=t("analytics.alerts_per_1k"),
                axis=alt.Axis(titleColor=_ACCENT)),
        tooltip=["day:T", alt.Tooltip("alerts_per_1k:Q", title=t("analytics.alerts_per_1k"))],
    )
    chart = alt.layer(bars, line).resolve_scale(y="independent").properties(height=280)
    st.altair_chart(chart, use_container_width=True)
    st.caption(t("analytics.s2_insight"))


def _section_acquisition(days: int) -> None:
    st.subheader(t("analytics.s3_title"))
    df = fetch_acquisition_trend(days)
    if df.empty:
        _empty()
        return
    df = df.copy()
    df["day"] = pd.to_datetime(df["day"])
    chart = (
        alt.Chart(df)
        .mark_area(opacity=0.75)
        .encode(
            x=alt.X("day:T", title=t("analytics.day")),
            y=alt.Y("new_customers:Q", title=t("analytics.new_customers"), stack=True),
            color=alt.Color("channel:N", title=t("analytics.channel")),
            tooltip=["day:T", "channel:N",
                     alt.Tooltip("new_customers:Q", title=t("analytics.new_customers"))],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(t("analytics.s3_insight"))


def _segmentation_chart(df: pd.DataFrame) -> None:
    if df.empty:
        _empty()
        return
    df = df.copy()
    volume = (
        alt.Chart(df)
        .mark_bar(color=_ACCENT)
        .encode(
            x=alt.X("volume:Q", title=t("analytics.metric_volume")),
            y=alt.Y("dim:N", sort="-x", title=""),
            tooltip=["dim:N", alt.Tooltip("volume:Q", title=t("analytics.metric_volume"), format=",.0f")],
        )
        .properties(height=max(160, 34 * len(df)))
    )
    rate = (
        alt.Chart(df)
        .mark_bar(color=_DANGER)
        .encode(
            x=alt.X("alerts_per_1k:Q", title=t("analytics.metric_alert_rate")),
            y=alt.Y("dim:N", sort="-x", title=""),
            tooltip=["dim:N", alt.Tooltip("alerts_per_1k:Q", title=t("analytics.metric_alert_rate"))],
        )
        .properties(height=max(160, 34 * len(df)))
    )
    c1, c2 = st.columns(2)
    c1.markdown(f"**{t('analytics.metric_volume')}**")
    c1.altair_chart(volume, use_container_width=True)
    c2.markdown(f"**{t('analytics.metric_alert_rate')}**")
    c2.altair_chart(rate, use_container_width=True)


def _section_segmentation() -> None:
    st.subheader(t("analytics.s4_title"))
    seg = fetch_segmentation()
    tabs = st.tabs([
        t("analytics.dim_segment"),
        t("analytics.dim_channel"),
        t("analytics.dim_txn_type"),
    ])
    with tabs[0]:
        _segmentation_chart(seg.get("by_segment", pd.DataFrame()))
    with tabs[1]:
        _segmentation_chart(seg.get("by_channel", pd.DataFrame()))
    with tabs[2]:
        _segmentation_chart(seg.get("by_txn_type", pd.DataFrame()))
    st.caption(t("analytics.s4_insight"))


def _section_geo() -> None:
    st.subheader(t("analytics.s5_title"))
    df = fetch_geo_corridors(limit=12)
    if df.empty:
        _empty()
        return
    df = df.copy()
    df["corridor"] = df["sender_country"].astype(str) + " → " + df["receiver_country"].astype(str)
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("volume:Q", title=t("analytics.volume")),
            y=alt.Y("corridor:N", sort="-x", title=t("analytics.corridor")),
            color=alt.Color("alert_count:Q", title=t("analytics.alerts"),
                            scale=alt.Scale(scheme="reds")),
            tooltip=[
                "corridor:N",
                alt.Tooltip("volume:Q", title=t("analytics.volume"), format=",.0f"),
                alt.Tooltip("txn_count:Q", title=t("analytics.txn_count")),
                alt.Tooltip("alert_count:Q", title=t("analytics.alerts")),
            ],
        )
        .properties(height=max(200, 30 * len(df)))
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(t("analytics.s5_insight"))


def _render_bands(bands: pd.DataFrame) -> None:
    if bands.empty:
        return
    order = ["Minimal (0-24)", "Low (25-49)", "Medium (50-74)", "High (75-100)"]
    df = bands.copy()
    df["incidence"] = (df["flagged_customers"] / df["active_customers"].replace(0, pd.NA) * 100).round(2)
    chart = (
        alt.Chart(df)
        .mark_bar(color=_DANGER)
        .encode(
            x=alt.X("band:N", sort=order, title=t("analytics.risk_band")),
            y=alt.Y("incidence:Q", title=t("analytics.incidence")),
            tooltip=[
                alt.Tooltip("band:N", title=t("analytics.risk_band")),
                alt.Tooltip("active_customers:Q", title=t("analytics.active_customers")),
                alt.Tooltip("flagged_customers:Q", title=t("analytics.alerts")),
                alt.Tooltip("incidence:Q", title=t("analytics.incidence")),
            ],
        )
        .properties(height=240)
    )
    st.markdown(f"**{t('analytics.risk_band')}**")
    st.altair_chart(chart, use_container_width=True)


def _render_pep(pep: pd.DataFrame) -> None:
    if pep.empty:
        return
    df = pep.copy()
    df["group"] = df["is_pep"].map({True: t("analytics.pep"), False: t("analytics.non_pep")})
    df["incidence"] = (df["flagged_customers"] / df["active_customers"].replace(0, pd.NA) * 100).round(2)
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("group:N", title=""),
            y=alt.Y("incidence:Q", title=t("analytics.incidence")),
            color=alt.Color("group:N", legend=None,
                            scale=alt.Scale(range=[_ACCENT, _DANGER])),
            tooltip=[
                alt.Tooltip("group:N", title=""),
                alt.Tooltip("active_customers:Q", title=t("analytics.active_customers")),
                alt.Tooltip("flagged_customers:Q", title=t("analytics.alerts")),
                alt.Tooltip("incidence:Q", title=t("analytics.incidence")),
            ],
        )
        .properties(height=240)
    )
    st.markdown(f"**{t('analytics.pep_title')}**")
    st.altair_chart(chart, use_container_width=True)


def _render_priority(priority: pd.DataFrame) -> None:
    if priority.empty:
        return
    chart = (
        alt.Chart(priority)
        .mark_bar(color=_ACCENT)
        .encode(
            x=alt.X("avg_priority:Q", title=t("analytics.avg_priority")),
            y=alt.Y("rule_name:N", sort="-x", title=t("analytics.rule")),
            tooltip=[
                alt.Tooltip("rule_name:N", title=t("analytics.rule")),
                alt.Tooltip("avg_priority:Q", title=t("analytics.avg_priority")),
                alt.Tooltip("alerts:Q", title=t("analytics.alerts")),
            ],
        )
        .properties(height=max(200, 26 * len(priority)))
    )
    st.markdown(f"**{t('analytics.priority_title')}**")
    st.altair_chart(chart, use_container_width=True)


def _section_risk() -> None:
    st.subheader(t("analytics.s6_title"))
    data = fetch_risk_analytics()
    bands = data.get("bands", pd.DataFrame())
    pep = data.get("pep", pd.DataFrame())
    priority = data.get("priority", pd.DataFrame())

    if bands.empty and pep.empty and priority.empty:
        _empty()
        return

    c1, c2 = st.columns(2)
    with c1:
        _render_bands(bands)
    with c2:
        _render_pep(pep)

    _render_priority(priority)
    st.caption(t("analytics.s6_insight"))


def render_analytics_page() -> None:
    top = st.columns([4, 1])
    top[0].header(t("analytics.title"))
    if top[1].button(t("analytics.refresh"), use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(t("analytics.intro"))

    window = st.radio(
        t("analytics.window_label"),
        [30, 90],
        index=0,
        horizontal=True,
        format_func=lambda d: t(f"analytics.window_{d}"),
        key="analytics_window",
    )

    sections = (
        lambda: _section_txn_trend(window),
        lambda: _section_alert_trend(window),
        lambda: _section_acquisition(max(window, 90)),
        _section_segmentation,
        _section_geo,
        _section_risk,
    )
    for render in sections:
        try:
            render()
        except Exception as exc:  # pragma: no cover - one bad query must not blank the page
            st.error(str(exc))
        st.divider()
