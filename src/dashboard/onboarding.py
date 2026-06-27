"""Onboarding / Overview landing page.

This is the recruiter-facing front door of the project. It frames the platform
as a real-time fraud-detection data pipeline that showcases data-engineering
skills (ingestion, streaming, modelling, orchestration, serving) on top of
genuine AML/fraud domain knowledge. First-person voice, English by default.
"""

from __future__ import annotations

import streamlit as st

from src.common.retention import load_retention
from src.dashboard.db import fetch_dashboard_kpis, fetch_table_counts
from src.dashboard.i18n import t


def _cards(items: list[dict], columns: int = 3, kind: str = "ob-card") -> None:
    rows = [items[i : i + columns] for i in range(0, len(items), columns)]
    for row in rows:
        cols = st.columns(columns)
        for col, item in zip(cols, row):
            tag = f"<div class='ob-tag'>{item['tag']}</div>" if item.get("tag") else ""
            num = f"<div class='ob-step-num'>{item['num']}</div>" if item.get("num") else ""
            icon = f"<span class='ob-card-icon'>{item['icon']}</span>" if item.get("icon") else ""
            col.markdown(
                f"""
                <div class='{kind}'>
                    {num}{icon}
                    <div class='ob-card-title'>{item['title']}</div>
                    <div class='ob-card-body'>{item['body']}</div>
                    {tag}
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.write("")


def _hero() -> None:
    st.markdown(
        f"""
        <div class='ob-hero'>
            <div class='ob-hero-badge'>{t('overview.hero.badge')}</div>
            <div class='ob-hero-title'>{t('overview.hero.title')}</div>
            <div class='ob-hero-sub'>{t('overview.hero.sub')}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _by_the_numbers() -> None:
    try:
        kpis = fetch_dashboard_kpis()
    except Exception as exc:
        st.warning(t("kpi.unavailable", error=str(exc)))
        kpis = {"customers": 1_233_000, "txns_today": 0, "scenarios": 8, "total_alerts": 0}

    st.markdown(
        f"<div class='ob-section-title'>{t('overview.numbers.title')}</div>",
        unsafe_allow_html=True,
    )
    cols = st.columns(4)
    cols[0].metric(t("overview.numbers.customers"), f"{kpis['customers']:,}")
    cols[1].metric(t("overview.numbers.txns"), f"{kpis['txns_today']:,}")
    cols[2].metric(t("overview.numbers.scenarios"), kpis["scenarios"])
    cols[3].metric(t("overview.numbers.alerts"), f"{kpis['total_alerts']:,}")
    st.caption(t("overview.numbers.live_hint"))

    facts = [
        ("~30 s", t("overview.facts.latency")),
        ("3-tier", t("overview.facts.layers")),
        ("9", t("overview.facts.services")),
        ("6 GB", t("overview.facts.vm")),
    ]
    fcols = st.columns(4)
    for col, (val, label) in zip(fcols, facts):
        col.markdown(
            f"<div class='ob-fact'><div class='ob-fact-val'>{val}</div>"
            f"<div class='ob-fact-label'>{label}</div></div>",
            unsafe_allow_html=True,
        )
    st.write("")


def _architecture() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.arch.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.arch.sub')}</div>",
        unsafe_allow_html=True,
    )
    nodes = [
        ("🏭", "n1"),
        ("🔀", "n2"),
        ("⚙️", "n3"),
        ("🗄️", "n4"),
        ("🧱", "n5"),
        ("📊", "n6"),
    ]
    parts = []
    for i, (icon, key) in enumerate(nodes):
        parts.append(
            f"<div class='arch-node'><div class='arch-node-icon'>{icon}</div>"
            f"<div class='arch-node-tech'>{t(f'overview.arch.{key}.tech')}</div>"
            f"<div class='arch-node-role'>{t(f'overview.arch.{key}.role')}</div></div>"
        )
        if i < len(nodes) - 1:
            parts.append("<div class='arch-arrow'>→</div>")
    st.markdown(f"<div class='arch-flow'>{''.join(parts)}</div>", unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class='arch-sidecar'>
            <div class='arch-sidecar-item'>{t('overview.arch.side1')}</div>
            <div class='arch-sidecar-item'>{t('overview.arch.side2')}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")


def _flow() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.flow.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.flow.sub')}</div>",
        unsafe_allow_html=True,
    )
    icons = ["🏦", "🔀", "🧠", "🚩", "🔎", "🤖", "🧹"]
    steps = [
        {
            "num": f"STEP {i}",
            "icon": icon,
            "title": t(f"overview.flow.s{i}.title"),
            "body": t(f"overview.flow.s{i}.body"),
            "tag": t(f"overview.flow.s{i}.tag"),
        }
        for i, icon in enumerate(icons, start=1)
    ]
    _cards(steps[:4], columns=4, kind="ob-step")
    _cards(steps[4:], columns=3, kind="ob-step")


def _data_model() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.model.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.model.sub')}</div>",
        unsafe_allow_html=True,
    )
    tiers = [
        ("bronze", "Kafka: transactions.raw"),
        ("silver", "aml.transactions\naml.account_window_metrics\naml.flagged_transactions\nstg_flagged_transactions"),
        ("gold", "gold_customer_risk_profile\ngold_account_risk_score\ngold_daily_fraud_summary"),
    ]
    cols = st.columns(3)
    for col, (tier, tables) in zip(cols, tiers):
        tbl_html = "".join(
            f"<span class='medallion-tbl'>{line}</span>" for line in tables.split("\n")
        )
        col.markdown(
            f"""
            <div class='medallion medallion-{tier}'>
                <div class='medallion-tier'>{t(f'overview.model.{tier}.tier')}</div>
                <div class='medallion-title'>{t(f'overview.model.{tier}.title')}</div>
                {tbl_html}
                <div class='medallion-note'>{t(f'overview.model.{tier}.note')}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.write("")
    st.caption(t("overview.model.lineage"))


def _scenarios() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.scenarios.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.scenarios.sub')}</div>",
        unsafe_allow_html=True,
    )
    scenario_keys = [
        ("🌍", "geo"),
        ("💰", "hv"),
        ("🐜", "smurf"),
        ("⚡", "velocity"),
        ("📈", "weekly"),
        ("👥", "peer"),
        ("😴", "dormant"),
        ("🪤", "mule"),
    ]
    scenarios = [
        {
            "icon": icon,
            "title": t(f"overview.sc.{key}.title"),
            "body": t(f"overview.sc.{key}.body"),
            "tag": t(f"overview.sc.{key}.tag"),
        }
        for icon, key in scenario_keys
    ]
    _cards(scenarios, columns=4)


def _engineering() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.eng.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.eng.sub')}</div>",
        unsafe_allow_html=True,
    )
    icons = ["🧮", "⚡", "🔁", "🛡️", "♻️", "🧊"]
    blocks = [
        {
            "icon": icon,
            "title": t(f"overview.eng.e{i}.title"),
            "body": t(f"overview.eng.e{i}.body"),
            "tag": t(f"overview.eng.e{i}.tag"),
        }
        for i, icon in enumerate(icons, start=1)
    ]
    _cards(blocks, columns=3)


def _business_impact() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.biz.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.biz.sub')}</div>",
        unsafe_allow_html=True,
    )
    icons = ["🧾", "🎯", "🏛️", "🧭"]
    blocks = [
        {
            "icon": icon,
            "title": t(f"overview.biz.b{i}.title"),
            "body": t(f"overview.biz.b{i}.body"),
        }
        for i, icon in enumerate(icons, start=1)
    ]
    _cards(blocks, columns=4)


def _technologies() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.tech.title')}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='ob-section-sub'>{t('overview.tech.sub')}</div>",
        unsafe_allow_html=True,
    )
    layers = [
        ("ingestion", "Python · Faker · Apache Kafka"),
        ("streaming", "Apache Spark — Structured Streaming"),
        ("storage", "PostgreSQL"),
        ("transform", "dbt (staging → gold models)"),
        ("orchestration", "Apache Airflow"),
        ("serving", "Streamlit"),
        ("ai", "GenAI / LLM (SAR drafting)"),
        ("platform", "Docker Compose · Oracle Cloud VM"),
    ]
    html = "".join(
        f"<div class='layer-row'><div class='layer-name'>{t(f'overview.layer.{key}')}</div>"
        f"<div class='layer-tools'>{tools}</div></div>"
        for key, tools in layers
    )
    st.markdown(html, unsafe_allow_html=True)
    st.write("")


def _data_and_protection() -> None:
    st.markdown(
        f"<div class='ob-section-title'>{t('overview.data.title')}</div>",
        unsafe_allow_html=True,
    )
    blocks = [
        {
            "icon": "👤",
            "title": t("overview.data.cust.title"),
            "body": t("overview.data.cust.body"),
            "tag": t("overview.data.cust.tag"),
        },
        {
            "icon": "🛡️",
            "title": t("overview.data.budget.title"),
            "body": t("overview.data.budget.body"),
            "tag": t("overview.data.budget.tag"),
        },
        {
            "icon": "♻️",
            "title": t("overview.data.retention.title"),
            "body": t("overview.data.retention.body"),
            "tag": t("overview.data.retention.tag"),
        },
    ]
    _cards(blocks, columns=3)

    cfg = load_retention()
    policies = cfg.get("policies", {})
    tables = [p.get("table") for p in policies.values() if p.get("table")]
    try:
        counts = fetch_table_counts(tuple(tables))
    except Exception:
        counts = {}
    rows = []
    for pol in policies.values():
        table = pol.get("table", "")
        rows.append(
            {
                t("retention.col_data"): pol.get("label", table),
                t("retention.col_retention"): f"{pol.get('value', '?')} {pol.get('unit', '')}".strip(),
                t("retention.col_rows"): f"{counts.get(table, 0):,}",
                t("overview.data.col_why"): pol.get("description", ""),
            }
        )
    if rows:
        import pandas as pd

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption(t("overview.data.guard_caption"))


def render_onboarding_page() -> None:
    _hero()
    _by_the_numbers()
    _architecture()
    _flow()
    _data_model()
    _scenarios()
    _engineering()
    _business_impact()
    _technologies()
    _data_and_protection()
    st.markdown("---")
    st.caption(t("overview.footer"))
