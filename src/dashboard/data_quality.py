"""Data quality rule definitions and live checks against aml.transactions."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.i18n import t

from src.dashboard.db import query_df

RULES_PATH = Path("/app/configs/data_quality_rules.json")
if not RULES_PATH.exists():
    RULES_PATH = Path(__file__).resolve().parents[2] / "configs" / "data_quality_rules.json"

FX_TO_EUR = {"EUR": 1.0, "USD": 0.92, "TRY": 0.027, "GBP": 1.17, "CHF": 1.05}


def load_dq_rule_catalog() -> list[dict]:
    if RULES_PATH.exists():
        with open(RULES_PATH) as f:
            return json.load(f).get("rules", [])
    return []


def _violations_sender_identity_country() -> pd.DataFrame:
    return query_df(
        """
        SELECT sender_identity_no AS entity_id,
               COUNT(DISTINCT sender_country) AS distinct_values,
               STRING_AGG(DISTINCT sender_country, ', ' ORDER BY sender_country) AS values_seen
        FROM aml.transactions
        WHERE sender_identity_no IS NOT NULL AND sender_country IS NOT NULL
        GROUP BY sender_identity_no
        HAVING COUNT(DISTINCT sender_country) > 1
        ORDER BY distinct_values DESC
        LIMIT 50
        """
    )


def _violations_sender_identity_name() -> pd.DataFrame:
    return query_df(
        """
        SELECT sender_identity_no AS entity_id,
               COUNT(DISTINCT sender_name) AS distinct_values,
               STRING_AGG(DISTINCT LEFT(sender_name, 40), ' | ') AS values_seen
        FROM aml.transactions
        WHERE sender_identity_no IS NOT NULL AND sender_name IS NOT NULL
        GROUP BY sender_identity_no
        HAVING COUNT(DISTINCT sender_name) > 1
        ORDER BY distinct_values DESC
        LIMIT 50
        """
    )


def _violations_receiver_identity_country() -> pd.DataFrame:
    return query_df(
        """
        SELECT receiver_identity_no AS entity_id,
               COUNT(DISTINCT receiver_country) AS distinct_values,
               STRING_AGG(DISTINCT receiver_country, ', ' ORDER BY receiver_country) AS values_seen
        FROM aml.transactions
        WHERE receiver_identity_no IS NOT NULL AND receiver_country IS NOT NULL
        GROUP BY receiver_identity_no
        HAVING COUNT(DISTINCT receiver_country) > 1
        ORDER BY distinct_values DESC
        LIMIT 50
        """
    )


def _violations_receiver_identity_name() -> pd.DataFrame:
    return query_df(
        """
        SELECT receiver_identity_no AS entity_id,
               COUNT(DISTINCT receiver_name) AS distinct_values,
               STRING_AGG(DISTINCT LEFT(receiver_name, 40), ' | ') AS values_seen
        FROM aml.transactions
        WHERE receiver_identity_no IS NOT NULL AND receiver_name IS NOT NULL
        GROUP BY receiver_identity_no
        HAVING COUNT(DISTINCT receiver_name) > 1
        ORDER BY distinct_values DESC
        LIMIT 50
        """
    )


def _violations_customer_branch() -> pd.DataFrame:
    return query_df(
        """
        WITH sender_branches AS (
            SELECT sender_customer_no AS customer_no, COUNT(DISTINCT sender_branch) AS n
            FROM aml.transactions
            WHERE sender_customer_no IS NOT NULL AND sender_branch IS NOT NULL
            GROUP BY sender_customer_no HAVING COUNT(DISTINCT sender_branch) > 1
        ),
        receiver_branches AS (
            SELECT receiver_customer_no AS customer_no, COUNT(DISTINCT receiver_branch) AS n
            FROM aml.transactions
            WHERE receiver_customer_no IS NOT NULL AND receiver_branch IS NOT NULL
            GROUP BY receiver_customer_no HAVING COUNT(DISTINCT receiver_branch) > 1
        )
        SELECT customer_no AS entity_id, n AS distinct_values, 'branch mismatch' AS values_seen
        FROM sender_branches
        UNION ALL
        SELECT customer_no, n, 'branch mismatch' FROM receiver_branches
        LIMIT 50
        """
    )


def _violations_fx_conversion() -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id AS entity_id, currency, amount, amount_eur,
               ROUND(
                 ABS(amount_eur - (
                   CASE currency
                     WHEN 'USD' THEN amount * 0.92
                     WHEN 'TRY' THEN amount * 0.027
                     WHEN 'GBP' THEN amount * 1.17
                     WHEN 'CHF' THEN amount * 1.05
                     ELSE amount
                   END
                 )) / NULLIF(amount_eur, 0) * 100, 2
               ) AS distinct_values
        FROM aml.transactions
        WHERE amount_eur IS NOT NULL AND amount > 0
          AND ABS(amount_eur - (
                CASE currency
                  WHEN 'USD' THEN amount * 0.92
                  WHEN 'TRY' THEN amount * 0.027
                  WHEN 'GBP' THEN amount * 1.17
                  WHEN 'CHF' THEN amount * 1.05
                  ELSE amount
                END
              )) / NULLIF(amount_eur, 0) > 0.01
        ORDER BY distinct_values DESC
        LIMIT 50
        """
    )


def _violations_required_fields() -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id AS entity_id, 1 AS distinct_values,
               TRIM(BOTH ', ' FROM CONCAT_WS(', ',
                 CASE WHEN sender_name IS NULL OR sender_name = '' THEN 'sender_name' END,
                 CASE WHEN sender_identity_no IS NULL OR sender_identity_no = '' THEN 'sender_identity_no' END,
                 CASE WHEN receiver_name IS NULL OR receiver_name = '' THEN 'receiver_name' END,
                 CASE WHEN receiver_identity_no IS NULL OR receiver_identity_no = '' THEN 'receiver_identity_no' END
               )) AS values_seen
        FROM aml.transactions
        WHERE sender_name IS NULL OR sender_name = ''
           OR sender_identity_no IS NULL OR sender_identity_no = ''
           OR receiver_name IS NULL OR receiver_name = ''
           OR receiver_identity_no IS NULL OR receiver_identity_no = ''
        LIMIT 50
        """
    )


def _violations_external_sender() -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id AS entity_id, 1 AS distinct_values,
               'missing external sender fields' AS values_seen
        FROM aml.transactions
        WHERE sender_customer_no IS NULL
          AND (sender_name IS NULL OR sender_name = ''
               OR sender_identity_no IS NULL OR sender_identity_no = '')
        LIMIT 50
        """
    )


CHECK_RUNNERS = {
    "DQ-001": _violations_sender_identity_country,
    "DQ-002": _violations_sender_identity_name,
    "DQ-003": _violations_receiver_identity_country,
    "DQ-004": _violations_receiver_identity_name,
    "DQ-005": _violations_customer_branch,
    "DQ-006": _violations_fx_conversion,
    "DQ-007": _violations_required_fields,
    "DQ-008": _violations_external_sender,
}


def run_all_checks() -> list[dict]:
    results = []
    for rule in load_dq_rule_catalog():
        rid = rule["id"]
        runner = CHECK_RUNNERS.get(rid)
        if not runner:
            continue
        try:
            violations = runner()
            count = len(violations)
        except Exception as exc:
            violations = pd.DataFrame()
            count = -1
            rule = {**rule, "error": str(exc)}
        results.append({**rule, "violation_count": count, "violations": violations})
    return results


def render_data_quality_panel() -> None:
    results = run_all_checks()
    if not results:
        st.info(t("dq.no_rules"))
        return

    passed = sum(1 for r in results if r["violation_count"] == 0)
    failed = sum(1 for r in results if r["violation_count"] > 0)
    errored = sum(1 for r in results if r["violation_count"] < 0)
    c1, c2, c3 = st.columns(3)
    c1.metric(t("dq.rules_passing"), passed)
    c2.metric(t("dq.rules_failing"), failed, delta=None if failed == 0 else f"-{failed}", delta_color="inverse")
    c3.metric(t("dq.check_errors"), errored)

    for r in results:
        count = r["violation_count"]
        if count < 0:
            status, color = "ERROR", "#f87171"
        elif count == 0:
            status, color = "PASS", "#86efac"
        else:
            status, color = "FAIL", "#fca5a5"
        with st.expander(f"[{r['id']}] {r['name']} — {status} ({max(count, 0)} violations)", expanded=count > 0):
            st.markdown(
                f'<span style="color:{color};font-weight:600">{status}</span> · '
                f'Severity: **{r["severity"]}** · Category: `{r["category"]}`',
                unsafe_allow_html=True,
            )
            st.caption(r["description"])
            if count > 0 and not r["violations"].empty:
                st.dataframe(r["violations"], use_container_width=True, hide_index=True)
            if r.get("error"):
                st.error(r["error"])
