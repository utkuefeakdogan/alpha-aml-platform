"""System Event Logs — central WARNING+ feed from every service + Airflow.

Read-only view over aml.event_log: 24h tiles, level/source/time filters, an
events-over-time chart and a detail table (with traceback/context expanders).
"""

from __future__ import annotations

import json

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.db import (
    fetch_event_log,
    fetch_event_log_sources,
    fetch_event_log_summary,
)
from src.dashboard.i18n import t

_ACCENT = "#38bdf8"
_DANGER = "#f87171"
_WARN = "#fbbf24"
_LEVELS = ("WARNING", "ERROR", "CRITICAL")

_RANGE_HOURS = {
    "1h": 1,
    "24h": 24,
    "7d": 168,
    "all": None,
}


def _level_color(level: str) -> str:
    return {"WARNING": _WARN, "ERROR": _DANGER, "CRITICAL": _DANGER}.get(level, _ACCENT)


def _render_tiles(summary: dict) -> None:
    cols = st.columns(3)
    cols[0].metric(t("log.tile_err"), f"{summary.get('err_24h', 0):,}")
    cols[1].metric(t("log.tile_warn"), f"{summary.get('warn_24h', 0):,}")
    cols[2].metric(t("log.tile_total"), f"{summary.get('total_24h', 0):,}")


def _render_time_chart(df: pd.DataFrame) -> None:
    if df.empty:
        return
    chart_df = df.copy()
    chart_df["ts"] = pd.to_datetime(chart_df["ts"])
    chart_df["bucket"] = chart_df["ts"].dt.floor("h")
    grouped = (
        chart_df.groupby(["bucket", "level"]).size().reset_index(name="count")
    )
    chart = (
        alt.Chart(grouped)
        .mark_bar()
        .encode(
            x=alt.X("bucket:T", title=t("log.chart_x")),
            y=alt.Y("count:Q", title=t("log.count")),
            color=alt.Color(
                "level:N",
                title=t("log.f_level"),
                scale=alt.Scale(
                    domain=list(_LEVELS),
                    range=[_WARN, _DANGER, "#b91c1c"],
                ),
            ),
            tooltip=["bucket:T", "level:N", alt.Tooltip("count:Q", title=t("log.count"))],
        )
        .properties(height=220)
    )
    st.markdown(f"**{t('log.chart_title')}**")
    st.altair_chart(chart, use_container_width=True)


def _render_by_source(summary: dict) -> None:
    by_source = summary.get("by_source")
    if not isinstance(by_source, pd.DataFrame) or by_source.empty:
        return
    chart = (
        alt.Chart(by_source)
        .mark_bar()
        .encode(
            x=alt.X("cnt:Q", title=t("log.count")),
            y=alt.Y("source:N", sort="-x", title=""),
            color=alt.Color(
                "level:N",
                title=t("log.f_level"),
                scale=alt.Scale(domain=list(_LEVELS), range=[_WARN, _DANGER, "#b91c1c"]),
            ),
            tooltip=["source:N", "level:N", alt.Tooltip("cnt:Q", title=t("log.count"))],
        )
        .properties(height=200)
    )
    st.markdown(f"**{t('log.by_source_title')}**")
    st.altair_chart(chart, use_container_width=True)


def _render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info(t("log.none"))
        return
    st.caption(t("log.showing", n=len(df)))
    view = df.rename(
        columns={
            "ts": t("log.col_ts"),
            "source": t("log.col_source"),
            "level": t("log.col_level"),
            "logger": t("log.col_logger"),
            "message": t("log.col_message"),
        }
    )[
        [
            t("log.col_ts"), t("log.col_source"), t("log.col_level"),
            t("log.col_logger"), t("log.col_message"),
        ]
    ]
    st.dataframe(view, use_container_width=True, hide_index=True)

    detailed = df[df["detail"].notna()]
    if not detailed.empty:
        with st.expander(t("log.detail")):
            for _, row in detailed.head(50).iterrows():
                detail = row["detail"]
                if isinstance(detail, (dict, list)):
                    body = json.dumps(detail, indent=2, default=str)
                else:
                    body = str(detail)
                st.markdown(f"**{row['ts']} · {row['source']} · {row['level']}**")
                st.code(body)


def render_event_log_page() -> None:
    top = st.columns([4, 1])
    top[0].header(t("log.title"))
    if top[1].button(t("log.refresh"), use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(t("log.intro"))

    summary = fetch_event_log_summary()
    _render_tiles(summary)
    st.divider()

    f1, f2, f3, f4 = st.columns([1.2, 1.2, 1, 1.6])
    levels = f1.multiselect(
        t("log.f_level"), list(_LEVELS), default=[], key="log_levels"
    )
    source_options = fetch_event_log_sources()
    sources = f2.multiselect(
        t("log.f_source"), source_options, default=[], key="log_sources"
    )
    range_key = f3.selectbox(
        t("log.f_time"),
        list(_RANGE_HOURS.keys()),
        index=1,
        format_func=lambda k: t(f"log.range_{k}"),
        key="log_range",
    )
    search = f4.text_input(
        t("log.f_search"), value="", placeholder=t("log.search_ph"), key="log_search"
    )

    df = fetch_event_log(
        levels=tuple(levels),
        sources=tuple(sources),
        since_hours=_RANGE_HOURS[range_key],
        search=search.strip() or None,
        limit=500,
    )

    chart_cols = st.columns(2)
    with chart_cols[0]:
        _render_time_chart(df)
    with chart_cols[1]:
        _render_by_source(summary)

    st.divider()
    _render_table(df)
