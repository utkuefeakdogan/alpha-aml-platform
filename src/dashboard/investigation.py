"""Banking investigation workspace — Customer 360 + alert triage."""

from __future__ import annotations

import streamlit as st

from src.dashboard.components import format_transactions_table, render_open_alerts_table
from src.dashboard.db import (
    fetch_alert_archive_transactions,
    fetch_alert_by_txn_id,
    fetch_alert_disposition,
    fetch_customer_addresses,
    fetch_customer_alerts_30d,
    fetch_customer_behavior,
    fetch_customer_open_alerts,
    fetch_customer_profile,
    fetch_customer_transactions_30d,
    fetch_top_open_alerts,
    resolve_investigation_target,
    save_alert_disposition,
)
from src.dashboard.i18n import t
from src.dashboard.sar_reporter import build_intelligence_summary, create_sar_from_context


def _mask_identity(val) -> str:
    if val is None or str(val).strip() == "":
        return "—"
    s = str(val)
    if len(s) <= 4:
        return "****"
    return s[:3] + "****" + s[-2:]


def _render_top_high_risk_alerts(on_investigate, *, show_header: bool = True) -> None:
    if show_header:
        st.markdown(f"#### {t('investigation.top_alerts')}")
        st.caption(t("investigation.top_alerts_caption"))
    try:
        top = fetch_top_open_alerts(limit=10)
        render_open_alerts_table(top, on_investigate, key_prefix="top_risk")
    except Exception as exc:
        st.warning(t("investigation.top_alerts_error", error=str(exc)))


def _render_other_high_risk_collapsed(on_investigate) -> None:
    """When investigating a specific target, keep the queue available but tucked away."""
    st.divider()
    with st.expander(t("investigation.other_high_risk"), expanded=False):
        st.caption(t("investigation.top_alerts_caption"))
        _render_top_high_risk_alerts(on_investigate, show_header=False)


def _render_customer_identity_tab(customer_id: str, profile: dict) -> None:
    customer = profile["customer"]
    spend = profile["spend"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(t("investigation.customer_id"), customer_id)
    c2.metric(
        t("investigation.kyc_score"),
        f"{float(customer['risk_score']):.1f}",
        help=t("investigation.kyc_help"),
    )
    c3.metric(t("investigation.spend_30d"), f"{float(spend['volume_30d']):,.0f}")
    c4.metric(t("investigation.segment"), str(customer.get("segment", "—")))

    st.markdown(f"#### {t('investigation.identity')}")
    idf = {
        "Field": [
            "Full Name", "Identity No", "Onboarding Date", "Onboarding Channel",
            "Branch", "PEP", "Status",
        ],
        "Value": [
            customer.get("name"),
            _mask_identity(customer.get("identity_no")),
            str(customer.get("onboarding_date", "—")),
            str(customer.get("onboarding_channel", "—")),
            str(customer.get("branch_id", "—")),
            t("common.yes") if customer.get("is_pep") else t("common.no"),
            str(customer.get("customer_status", "active")),
        ],
    }
    st.table(idf)

    st.markdown(f"#### {t('investigation.addresses')}")
    addresses = fetch_customer_addresses(customer_id)
    if addresses.empty:
        st.info(t("investigation.no_addresses"))
    else:
        st.dataframe(addresses, use_container_width=True, hide_index=True)

    st.markdown(f"#### {t('investigation.window_metrics')}")
    if profile["windows"].empty:
        st.info(t("investigation.no_metrics"))
    else:
        st.dataframe(profile["windows"], use_container_width=True, hide_index=True)

    st.markdown(f"#### {t('investigation.recent_flags')}")
    if profile["flags"].empty:
        st.info(t("investigation.no_flags"))
    else:
        st.dataframe(profile["flags"], use_container_width=True, hide_index=True)


def _render_customer_txn_tab(
    customer_id: str,
    alert_txn_id: str | None = None,
    rule_name: str | None = None,
) -> None:
    if alert_txn_id and rule_name:
        st.markdown(f"#### {t('investigation.alert_archive_txn')}")
        txns = fetch_alert_archive_transactions(customer_id, alert_txn_id, rule_name)
        n = len(txns)
        if n:
            st.caption(t("investigation.alert_archive_caption", count=n))
        else:
            st.info(t("investigation.alert_archive_fallback"))
            txns = fetch_customer_transactions_30d(customer_id)
    else:
        st.markdown(f"#### {t('investigation.last_30d')}")
        txns = fetch_customer_transactions_30d(customer_id)

    if txns.empty:
        st.info(t("investigation.no_txns_30d"))
    else:
        st.dataframe(format_transactions_table(txns), use_container_width=True, hide_index=True)


def _render_alert_details_tab(
    customer_id: str,
    profile: dict,
    alert,
    target_txn: str,
    target_rule: str,
) -> None:
    st.markdown(f"#### {t('investigation.why_triggered')}")
    st.write(f"**{t('investigation.rule')}:** {alert['rule_name']}")
    st.write(f"**{t('investigation.detail')}:** {alert.get('rule_detail', '—')}")
    st.write(f"**{t('investigation.window')}:** {alert.get('window_type', '—')}")

    a1, a2 = st.columns(2)
    priority = alert.get("alert_priority_score")
    a1.metric(
        t("investigation.alert_priority"),
        f"{float(priority):.1f}" if priority is not None and str(priority) != "nan" else "—",
        help=t("investigation.priority_help"),
    )
    a2.metric(
        t("investigation.kyc_score"),
        f"{float(profile['customer']['risk_score']):.1f}",
        help=t("investigation.kyc_help"),
    )

    disposition = fetch_alert_disposition(target_txn, target_rule)
    if disposition is not None:
        st.info(
            t(
                "investigation.archived_banner",
                disposition=disposition["disposition"],
                notes=disposition.get("analyst_notes") or "—",
                at=disposition.get("created_at"),
            )
        )

    behavior = fetch_customer_behavior(customer_id)
    if behavior is not None:
        st.markdown(f"#### {t('investigation.behavior_30d')}")
        b1, b2, b3, b4 = st.columns(4)
        b1.metric(t("investigation.txn_count_30d"), int(behavior.get("txn_count_30d", 0)))
        b2.metric(t("investigation.volume_30d"), f"{float(behavior.get('volume_30d', 0)):,.0f}")
        b3.metric(t("investigation.counterparties"), int(behavior.get("distinct_counterparties_30d", 0)))
        b4.metric(t("investigation.primary_country"), str(behavior.get("primary_sender_country") or "—"))
    else:
        st.markdown(f"#### {t('investigation.behavior_30d')}")
        s = profile["spend"]
        b1, b2, b3 = st.columns(3)
        b1.metric(t("investigation.txn_count_30d"), int(s.get("txn_count_30d", 0)))
        b2.metric(t("investigation.volume_30d"), f"{float(s.get('volume_30d', 0)):,.0f}")
        b3.metric(t("investigation.avg_amount"), f"{float(s.get('avg_amount_30d', 0)):,.0f}")

    st.markdown(f"#### {t('investigation.related_alerts')}")
    related = fetch_customer_alerts_30d(customer_id)
    if related.empty:
        st.info(t("investigation.no_other_alerts"))
    else:
        st.dataframe(related, use_container_width=True, hide_index=True)


def _render_take_action_tab(
    customer_id: str,
    alert,
    target_txn: str,
    target_rule: str,
    read_only: bool,
) -> None:
    brief = build_intelligence_summary(customer_id, str(alert["rule_name"]), target_txn)
    st.markdown(f"#### {t('investigation.intelligence_brief')}")
    st.text_area(
        "brief",
        brief,
        height=420,
        disabled=True,
        label_visibility="collapsed",
    )

    if read_only:
        st.info(t("investigation.archived_banner", disposition="archived", notes="—", at="—"))
        return

    analyst_name = st.text_input(
        t("investigation.analyst_name"),
        placeholder=t("investigation.analyst_name_placeholder"),
        key="inv_analyst_name",
    )
    notes = st.text_area(
        t("investigation.notes"),
        placeholder=t("investigation.notes_placeholder"),
        height=100,
        key="inv_page_notes",
    )
    can_act = bool(analyst_name and analyst_name.strip())
    if not can_act:
        st.caption(t("investigation.analyst_name_required"))

    a1, a2 = st.columns(2)
    with a1:
        if st.button(
            t("investigation.generate_sar"),
            type="primary",
            use_container_width=True,
            disabled=not can_act,
        ):
            note_block = f"\nNotes: {notes}" if notes else ""
            rid, txt = create_sar_from_context(
                customer_id,
                target_txn,
                str(alert["rule_name"]),
                str(alert.get("rule_detail", "")) + note_block,
                analyst_name.strip(),
            )
            disp_notes = f"[Analyst: {analyst_name.strip()}] {notes}".strip()
            save_alert_disposition(target_txn, str(alert["rule_name"]), "sar_filed", disp_notes)
            st.session_state.sar_text = txt
            st.session_state.sar_rid = rid
            st.success(t("investigation.sar_filed", rid=rid))
    with a2:
        if st.button(
            t("investigation.flag_non_alert"),
            use_container_width=True,
            disabled=not can_act,
        ):
            disp_notes = f"[Analyst: {analyst_name.strip()}] {notes}".strip()
            save_alert_disposition(target_txn, str(alert["rule_name"]), "false_positive", disp_notes)
            st.success(t("investigation.marked_non_alert"))
            st.session_state.nav_target = "SAR Archive"
            st.query_params.clear()
            st.rerun()


def _render_customer_mode(customer_id: str, on_investigate_alert) -> None:
    profile = fetch_customer_profile(customer_id)
    if not profile:
        st.error(t("investigation.customer_missing", cid=customer_id))
        return

    customer = profile["customer"]
    st.caption(
        f"{customer.get('name')} · Branch {customer.get('branch_id')} · "
        f"PEP: {t('common.yes') if customer.get('is_pep') else t('common.no')}"
    )

    tab_identity, tab_txn, tab_alerts = st.tabs(
        [t("investigation.tab_identity"), t("investigation.tab_txn"), t("investigation.tab_open_alerts")]
    )
    with tab_identity:
        _render_customer_identity_tab(customer_id, profile)
    with tab_txn:
        _render_customer_txn_tab(customer_id)
    with tab_alerts:
        st.markdown(f"#### {t('investigation.customer_open_alerts')}")
        open_alerts = fetch_customer_open_alerts(customer_id)
        if open_alerts.empty:
            st.info(t("investigation.no_open_for_customer"))
        else:
            render_open_alerts_table(open_alerts, on_investigate_alert, key_prefix="cust_open")


def render_investigation_page(
    txn_id: str | None,
    rule_name: str | None,
    on_investigate_alert,
) -> None:
    st.title(t("investigation.title"))

    prefill = st.session_state.pop("investigation_prefill", None)
    default_search = prefill or txn_id or ""

    st.text_input(
        t("investigation.search"),
        value=default_search,
        placeholder=t("investigation.search_placeholder"),
        key="inv_search",
    )

    search = st.session_state.get("inv_search", "")
    mode = "idle"
    target_txn = txn_id
    target_rule = rule_name
    customer_id: str | None = None

    if search and search.strip():
        resolved = resolve_investigation_target(search.strip())
        if resolved:
            if resolved.get("mode") == "customer":
                mode = "customer"
                customer_id = resolved["customer_id"]
                target_txn = None
                target_rule = None
            else:
                mode = "alert"
                target_txn = resolved["txn_id"]
                target_rule = resolved["rule_name"]
                customer_id = resolved.get("customer_id")
        elif not (txn_id and rule_name):
            st.warning(t("investigation.not_found"))
            return
    elif txn_id and rule_name:
        mode = "alert"

    if mode == "idle":
        _render_top_high_risk_alerts(on_investigate_alert)
        st.info(t("investigation.idle"))
        return

    if mode == "customer" and customer_id:
        _render_customer_mode(customer_id, on_investigate_alert)
        _render_other_high_risk_collapsed(on_investigate_alert)
        return

    if not target_txn:
        return

    alert = fetch_alert_by_txn_id(target_txn, target_rule)
    if alert is None:
        st.error(t("investigation.alert_not_found"))
        return

    customer_id = str(alert.get("customer_id") or alert.get("account_id") or "")
    if not customer_id:
        st.error(t("investigation.no_customer"))
        return

    profile = fetch_customer_profile(customer_id)
    if not profile:
        st.error(t("investigation.customer_missing", cid=customer_id))
        return

    read_only = fetch_alert_disposition(target_txn, str(alert["rule_name"])) is not None
    caption = (
        f"Alert `{target_txn[:8]}…` · Rule **{alert['rule_name']}** · "
        f"Flagged {alert['flagged_at']}"
    )
    if read_only:
        caption += t("investigation.read_only")
    st.caption(caption)

    tab_identity, tab_txn, tab_alert, tab_action = st.tabs(
        [
            t("investigation.tab_identity"),
            t("investigation.tab_txn"),
            t("investigation.tab_alert"),
            t("investigation.tab_action"),
        ]
    )
    with tab_identity:
        _render_customer_identity_tab(customer_id, profile)
    with tab_txn:
        _render_customer_txn_tab(customer_id, target_txn, str(alert["rule_name"]))
    with tab_alert:
        _render_alert_details_tab(
            customer_id, profile, alert, target_txn, str(alert["rule_name"])
        )
    with tab_action:
        _render_take_action_tab(
            customer_id, alert, target_txn, str(alert["rule_name"]), read_only
        )

    if st.session_state.get("sar_text"):
        st.subheader(t("investigation.generated_sar"))
        st.text_area(t("investigation.sar_document"), st.session_state.sar_text, height=280)

    _render_other_high_risk_collapsed(on_investigate_alert)
