"""Scenarios — read-only AML scenario showcase (typology, window, live thresholds)."""

from __future__ import annotations

import html
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src.dashboard.db import fetch_ml_latest_run
from src.dashboard.i18n import t
from src.dashboard.rules_manager import load_rules
from src.dashboard.scenario_catalog import load_scenario_catalog


def _fmt_eur(value: float) -> str:
    return f"{float(value):,.0f} EUR"


def _humanize_age(ts) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    age = int((pd.Timestamp(datetime.now(timezone.utc)) - ts).total_seconds())
    if age < 3600:
        return t("rm.ago_m", n=max(age // 60, 0))
    if age < 86400:
        return t("rm.ago_h", n=age // 3600)
    return t("rm.ago_d", n=age // 86400)


def _scenario_view(scenario: dict, rules: dict) -> dict:
    """Map a catalog scenario to its AML typology, window and live thresholds."""
    rule = scenario.get("rule_name", "")
    mw = rules.get("multi_window", {})
    view: dict = {"typology": "scenarios.typ.generic", "window": "scenarios.win.transaction",
                  "detect": ("scenarios.detect.generic", {}), "params": []}

    if rule == "geographic":
        geo = rules.get("geographic", {})
        view["typology"] = "scenarios.typ.geographic"
        view["detect"] = ("scenarios.detect.geographic", {})
        view["params"] = [
            ("scenarios.param.blocked", ", ".join(geo.get("blocked_countries", [])) or "—"),
            ("scenarios.param.high_risk", ", ".join(geo.get("high_risk_countries", [])) or "—"),
        ]
    elif rule == "high_value":
        thr = float(rules.get("high_value", {}).get("threshold_eur", 10000))
        view["typology"] = "scenarios.typ.high_value"
        view["detect"] = ("scenarios.detect.high_value", {"amount": _fmt_eur(thr)})
        view["params"] = [("scenarios.param.hv_threshold", _fmt_eur(thr))]
    elif rule == "smurfing":
        sm = rules.get("smurfing", {})
        cnt = int(sm.get("weekly_small_txn_count", 12))
        amt = float(sm.get("weekly_small_txn_threshold_eur", 500))
        view["typology"] = "scenarios.typ.smurfing"
        view["window"] = "scenarios.win.weekly"
        view["detect"] = ("scenarios.detect.smurfing", {"count": cnt, "amount": _fmt_eur(amt)})
        view["params"] = [
            ("scenarios.param.smurf_count", str(cnt)),
            ("scenarios.param.smurf_threshold", _fmt_eur(amt)),
        ]
    elif rule == "daily_velocity":
        cnt = int(mw.get("daily_velocity_max", 5))
        amt = float(mw.get("daily_velocity_max_amount_eur", 1000))
        view["typology"] = "scenarios.typ.daily_velocity"
        view["window"] = "scenarios.win.daily"
        view["detect"] = ("scenarios.detect.daily_velocity", {"count": cnt, "amount": _fmt_eur(amt)})
        view["params"] = [
            ("scenarios.param.daily_velocity", str(cnt)),
            ("scenarios.param.daily_amount_cap", _fmt_eur(amt)),
        ]
    elif rule == "weekly_volume":
        vol = float(mw.get("weekly_volume_max_eur", 10000))
        view["typology"] = "scenarios.typ.weekly_volume"
        view["window"] = "scenarios.win.weekly"
        view["detect"] = ("scenarios.detect.weekly_volume", {"amount": _fmt_eur(vol)})
        view["params"] = [("scenarios.param.weekly_volume", _fmt_eur(vol))]
    elif rule == "monthly_peer_anomaly":
        base = int(mw.get("monthly_peer_baseline_txn_count", 8))
        mult = float(mw.get("monthly_peer_anomaly_multiplier", 2.5))
        threshold = int(base * mult)
        view["typology"] = "scenarios.typ.monthly_peer"
        view["window"] = "scenarios.win.monthly"
        view["detect"] = (
            "scenarios.detect.monthly_peer",
            {"threshold": threshold, "baseline": base, "mult": mult},
        )
        view["params"] = [
            ("scenarios.param.monthly_baseline", str(base)),
            ("scenarios.param.monthly_mult", f"{mult:g}x"),
            ("scenarios.param.monthly_threshold", str(threshold)),
        ]
    elif rule == "dormant_reactivation":
        mn = float(rules.get("dormant_reactivation", {}).get("min_amount_eur", 3000))
        view["typology"] = "scenarios.typ.dormant"
        view["detect"] = ("scenarios.detect.dormant", {"amount": _fmt_eur(mn)})
        view["params"] = [("scenarios.param.dormant_min", _fmt_eur(mn))]
    elif rule == "mule_inbound":
        mu = rules.get("mule_inbound", {})
        senders = int(mu.get("min_distinct_senders", 5))
        amt = float(mu.get("min_total_amount_eur", 500))
        view["typology"] = "scenarios.typ.mule"
        view["window"] = "scenarios.win.batch24h"
        view["detect"] = ("scenarios.detect.mule", {"senders": senders, "amount": _fmt_eur(amt)})
        view["params"] = [
            ("scenarios.param.mule_senders", str(senders)),
            ("scenarios.param.mule_min", _fmt_eur(amt)),
        ]
    return view


def _render_card(scenario: dict, rules: dict) -> None:
    view = _scenario_view(scenario, rules)
    title = html.escape(scenario.get("title", scenario.get("id", "")))
    desc = html.escape(scenario.get("description", ""))
    typology = html.escape(t(view["typology"]))
    window = html.escape(t(view["window"]))
    dkey, dkw = view["detect"]
    detect = html.escape(t(dkey, **dkw))
    inject = int(scenario.get("max_txns_per_inject", 1))
    enabled = bool(scenario.get("enabled", True))
    status_cls = "scn-on" if enabled else "scn-off"
    status_txt = html.escape(t("scenarios.enabled") if enabled else t("scenarios.disabled"))

    params_html = "".join(
        f'<div class="scn-param"><span class="scn-pk">{html.escape(t(lk))}</span>'
        f'<span class="scn-pv">{html.escape(str(pv))}</span></div>'
        for lk, pv in view["params"]
    )

    st.markdown(
        f"""
        <div class="scn-card">
          <div class="scn-head">
            <span class="scn-title">{title}</span>
            <span class="scn-badge scn-typ">{typology}</span>
            <span class="scn-badge {status_cls}">{status_txt}</span>
          </div>
          <div class="scn-desc">{desc}</div>
          <div class="scn-detect">{detect}</div>
          <div class="scn-meta">
            <span class="scn-chip">{html.escape(t('scenarios.window'))}: {window}</span>
            <span class="scn-chip">{html.escape(t('scenarios.injection', count=inject))}</span>
          </div>
          <div class="scn-params">{params_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_ml_card() -> None:
    """9th, always-on scenario: the ML risk layer (model-driven, no fixed threshold).

    Reads live stats from the latest training run; details live on Risk Models.
    """
    run = fetch_ml_latest_run()
    outlier_pct = float(run.get("contamination") or 0.05) * 100 if run else 5.0

    if run:
        n_anom = int(run.get("n_anomalies") or 0)
        n_samples = int(run.get("n_samples") or 0)
        last_run = t("scenarios.ml.lastrun_val", anomalies=f"{n_anom:,}", samples=f"{n_samples:,}")
        roc = run.get("roc_auc")
        quality = f"{float(roc):.3f}" if roc is not None and not pd.isna(roc) else "—"
        age = _humanize_age(run.get("trained_at"))
        refreshed = age if age else t("scenarios.ml.p_none")
        n_features = int(run.get("n_features") or 12)
    else:
        last_run = t("scenarios.ml.p_none")
        quality = "—"
        refreshed = t("scenarios.ml.p_none")
        n_features = 12

    params = [
        ("scenarios.ml.p_model", t("scenarios.ml.model")),
        ("scenarios.ml.p_features", str(n_features)),
        ("scenarios.ml.p_outlier", f"{outlier_pct:.0f}%"),
        ("scenarios.ml.p_lastrun", last_run),
        ("scenarios.ml.p_quality", quality),
        ("scenarios.ml.p_refreshed", refreshed),
    ]
    params_html = "".join(
        f'<div class="scn-param"><span class="scn-pk">{html.escape(t(lk))}</span>'
        f'<span class="scn-pv">{html.escape(str(pv))}</span></div>'
        for lk, pv in params
    )

    st.markdown(
        f"""
        <div class="scn-card scn-card-ml">
          <div class="scn-head">
            <span class="scn-title">{html.escape(t('scenarios.ml.title'))}</span>
            <span class="scn-badge scn-ml">{html.escape(t('scenarios.ml.model_badge'))}</span>
            <span class="scn-badge scn-on">{html.escape(t('scenarios.ml.status'))}</span>
          </div>
          <div class="scn-desc">{html.escape(t('scenarios.ml.desc'))}</div>
          <div class="scn-detect">{html.escape(t('scenarios.ml.detect', pct=f'{outlier_pct:.0f}'))}</div>
          <div class="scn-meta">
            <span class="scn-chip">{html.escape(t('scenarios.window'))}: {html.escape(t('scenarios.ml.window'))}</span>
            <span class="scn-chip">{html.escape(t('scenarios.ml.details_hint'))}</span>
          </div>
          <div class="scn-params">{params_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_scenarios_page() -> None:
    st.header(t("scenarios.title"))
    st.caption(t("scenarios.intro"))

    rules = load_rules()
    scenarios = load_scenario_catalog()

    cols = st.columns(2)
    for idx, scenario in enumerate(scenarios):
        with cols[idx % 2]:
            _render_card(scenario, rules)
    # 9th, always-on ML scenario (model-driven; no injector, no alerts written).
    with cols[len(scenarios) % 2]:
        _render_ml_card()

    st.divider()
    st.subheader(t("rule_builder.propose_title"))
    st.text_area(
        t("rule_builder.propose_hint"),
        height=120,
        key="rb_propose_text",
        placeholder=t("rule_builder.propose_placeholder"),
    )
    st.button(
        t("rule_builder.propose_submit"),
        disabled=True,
        help=t("rule_builder.propose_disabled_help"),
    )
    st.info(t("rule_builder.propose_coming_soon"))


# Backwards-compatible alias (nav dispatch may still import the old name).
render_rule_builder_page = render_scenarios_page
