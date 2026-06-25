"""Alpha AML Enterprise Compliance Dashboard."""

from __future__ import annotations

import streamlit as st

from src.dashboard.components import (
    render_airflow_footer,
    render_data_pipeline_panel,
    render_alerts_by_rule_bar,
    render_kyc_kpi_row,
    render_live_recent_alerts,
    render_paginated_live_transactions_feed,
    render_retention_panel,
    render_risk_pie_chart,
)
from src.dashboard.data_quality import render_data_quality_panel
from src.dashboard.freesql import render_freesql_page
from src.dashboard.i18n import render_language_selector, t
from src.dashboard.investigation import render_investigation_page
from src.dashboard.onboarding import render_onboarding_page
from src.dashboard.rule_builder_ui import render_scenarios_page
from src.dashboard.sar_archive import render_sar_archive_page
from src.dashboard.styles import DARK_THEME_CSS
from src.dashboard.system_health import render_system_health_page

st.set_page_config(page_title="Alpha AML Platform", page_icon="🛡️", layout="wide")


def _theme() -> None:
    st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)


def _apply_nav_target() -> None:
    target = st.session_state.pop("nav_target", None)
    if target:
        st.session_state.sidebar_page = target


def _nav_to(page: str) -> None:
    st.session_state.nav_target = page
    st.rerun()


def _open_investigation(txn_id: str, rule_name: str) -> None:
    st.session_state.nav_target = "Investigation"
    st.query_params.update(id=txn_id, rule=rule_name)
    st.rerun()


def _open_investigation_customer(customer_id: str) -> None:
    st.session_state.nav_target = "Investigation"
    st.session_state.investigation_prefill = customer_id
    st.query_params.clear()
    st.rerun()


def _back_to_monitoring() -> None:
    st.session_state.nav_target = "Monitoring"
    st.query_params.clear()
    st.rerun()


def page_monitoring() -> None:
    st.header(t("monitoring.title"))
    render_kyc_kpi_row(_nav_to)

    chart_left, chart_right = st.columns(2)
    with chart_left:
        st.markdown(f"**{t('risk.title')}**")
        try:
            render_risk_pie_chart(embedded=True, chart_height=280)
        except Exception as exc:
            st.error(str(exc))
    with chart_right:
        st.markdown(f"**{t('monitoring.alerts_by_rule')}**")
        try:
            render_alerts_by_rule_bar(compact=True, chart_height=280)
        except Exception as exc:
            st.error(str(exc))
    st.caption(t("risk.caption"))

    st.subheader(t("monitoring.live_feeds"))
    st.markdown(f"**{t('monitoring.recent_alerts')}**")
    try:
        render_live_recent_alerts(_open_investigation, limit=5)
    except Exception as exc:
        st.error(str(exc))

    st.markdown(f"**{t('monitoring.all_transactions')}**")
    try:
        render_paginated_live_transactions_feed(_open_investigation_customer, page_size=10)
    except Exception as exc:
        st.error(str(exc))

    render_data_pipeline_panel()
    render_retention_panel()
    render_airflow_footer()


def page_data_quality() -> None:
    st.header(t("dq.title"))
    render_data_quality_panel()


def main() -> None:
    _theme()
    for k in ("sar_text", "sar_rid"):
        if k not in st.session_state:
            st.session_state[k] = None
    if "locale" not in st.session_state:
        st.session_state.locale = "en"
    if "sidebar_page" not in st.session_state:
        st.session_state.sidebar_page = "Overview"

    _apply_nav_target()

    nav_pages = [
        "Overview",
        "Monitoring",
        "Investigation",
        "SAR Archive",
        "Scenarios",
        "Data Quality",
        "System Health",
        "SQL Explorer",
    ]
    inv_id = st.query_params.get("id")
    inv_rule = st.query_params.get("rule")

    st.sidebar.title("Alpha AML Platform")
    render_language_selector()
    page = st.sidebar.radio(
        "Navigation",
        nav_pages,
        key="sidebar_page",
        format_func=lambda k: t(f"nav.{k}"),
    )

    if page == "Monitoring" and (inv_id or inv_rule):
        st.query_params.clear()
        st.rerun()

    if page == "Investigation":
        render_investigation_page(inv_id, inv_rule, _open_investigation)
        if st.sidebar.button(t("nav.back_monitoring")):
            _back_to_monitoring()
        return

    if page == "SAR Archive":
        render_sar_archive_page(_open_investigation, _open_investigation)
        return

    {
        "Overview": render_onboarding_page,
        "Monitoring": page_monitoring,
        "Scenarios": render_scenarios_page,
        "Data Quality": page_data_quality,
        "System Health": render_system_health_page,
        "SQL Explorer": render_freesql_page,
    }[page]()


if __name__ == "__main__":
    main()
