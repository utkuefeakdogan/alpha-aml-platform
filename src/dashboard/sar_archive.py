"""SAR Archive — open alert queue, archived dispositions, filed SAR reports."""

from __future__ import annotations

import streamlit as st

from src.dashboard.i18n import t

from src.dashboard.components import render_open_alerts_table
from src.dashboard.db import (
    fetch_archived_alerts,
    fetch_archived_alerts_count,
    fetch_open_alerts_count,
    fetch_sar_reports,
    fetch_sar_reports_count,
)


def _outcome_label(disposition: str) -> str:
    if disposition == "sar_filed":
        return t("sar_archive.outcome_sar")
    if disposition == "false_positive":
        return t("sar_archive.outcome_whiteflag")
    return disposition


def render_archived_alerts_table(
    alerts,
    on_view,
    key_prefix: str = "arch",
) -> None:
    if alerts.empty:
        st.info(t("sar_archive.no_archived"))
        return
    header = st.columns([3, 2, 2, 2, 3, 2, 1])
    for col, label in zip(
        header,
        [t("alert.col_id"), t("alert.col_customer"), t("alert.col_rule"), t("sar_archive.col_outcome"), t("sar_archive.col_notes"), t("sar_archive.col_archived"), ""],
    ):
        col.markdown(f"**{label}**")
    for idx, row in alerts.iterrows():
        alert_id = str(row["alert_id"])
        rule = str(row["rule_name"])
        cols = st.columns([3, 2, 2, 2, 3, 2, 1])
        cols[0].markdown(f"`{alert_id}`")
        cols[1].write(str(row.get("customer_name") or "—"))
        cols[2].write(rule)
        cols[3].write(_outcome_label(str(row.get("disposition") or "")))
        notes = str(row.get("analyst_notes") or "")[:80]
        cols[4].write(notes or "—")
        cols[5].write(str(row.get("archived_at") or "—")[:19])
        if cols[6].button(t("common.view_btn"), key=f"{key_prefix}_view_{alert_id}_{rule}_{idx}"):
            on_view(alert_id, rule)


def render_paginated_archived_alerts(on_view, page_size: int = 10) -> None:
    if "archived_alerts_offset" not in st.session_state:
        st.session_state.archived_alerts_offset = 0
    offset = int(st.session_state.archived_alerts_offset)
    total = fetch_archived_alerts_count()
    page = fetch_archived_alerts(limit=page_size, offset=offset)
    start = offset + 1 if total else 0
    end = min(offset + page_size, total)
    st.caption(t("sar_archive.showing_archived", start=start, end=end, total=total))
    render_archived_alerts_table(page, on_view, key_prefix="arch_page")
    nav1, _, nav3 = st.columns([1, 2, 1])
    with nav1:
        if st.button(t("common.previous"), disabled=offset == 0, key="arch_prev"):
            st.session_state.archived_alerts_offset = max(0, offset - page_size)
            st.rerun()
    with nav3:
        if st.button(t("common.next"), disabled=offset + page_size >= total, key="arch_next"):
            st.session_state.archived_alerts_offset = offset + page_size
            st.rerun()


def render_paginated_sar_reports(page_size: int = 10) -> None:
    if "sar_reports_offset" not in st.session_state:
        st.session_state.sar_reports_offset = 0
    offset = int(st.session_state.sar_reports_offset)
    total = fetch_sar_reports_count()
    reports = fetch_sar_reports(limit=page_size, offset=offset)
    start = offset + 1 if total else 0
    end = min(offset + page_size, total)
    st.caption(t("sar_archive.showing_sar", start=start, end=end, total=total))
    if reports.empty:
        st.info(t("sar_archive.no_sar"))
    else:
        for _, row in reports.iterrows():
            rid = str(row["report_id"])
            filed_by = row.get("filed_by") or "—"
            with st.expander(
                f"{rid} · {row.get('customer_id') or '—'} · "
                f"{t('sar_archive.filed_by')}: {filed_by} · "
                f"{t('sar_archive.filed_at')}: {str(row['created_at'])[:19]}"
            ):
                st.text_area(
                    t("sar_archive.report_text"),
                    str(row.get("report_text") or ""),
                    height=240,
                    disabled=True,
                    key=f"sar_txt_{rid}",
                )
    nav1, _, nav3 = st.columns([1, 2, 1])
    with nav1:
        if st.button(t("common.previous"), disabled=offset == 0, key="sar_prev"):
            st.session_state.sar_reports_offset = max(0, offset - page_size)
            st.rerun()
    with nav3:
        if st.button(t("common.next"), disabled=offset + page_size >= total, key="sar_next"):
            st.session_state.sar_reports_offset = offset + page_size
            st.rerun()


def render_sar_archive_page(
    on_investigate,
    on_view_archived,
) -> None:
    st.header(t("sar_archive.title"))

    try:
        open_cnt = fetch_open_alerts_count()
        arch_cnt = fetch_archived_alerts_count()
        sar_cnt = fetch_sar_reports_count()
    except Exception as exc:
        st.error(str(exc))
        return

    k1, k2, k3 = st.columns(3)
    k1.metric(t("sar_archive.open_alerts"), open_cnt)
    k2.metric(t("sar_archive.archived"), arch_cnt)
    k3.metric(t("sar_archive.sar_reports"), sar_cnt)

    st.subheader(t("sar_archive.all_open"))
    from src.dashboard.components import render_paginated_open_alerts

    render_paginated_open_alerts(on_investigate, page_size=10)

    st.subheader(t("sar_archive.archived_alerts"))
    render_paginated_archived_alerts(on_view_archived, page_size=10)

    st.subheader(t("sar_archive.sar_reports_section"))
    render_paginated_sar_reports(page_size=10)
