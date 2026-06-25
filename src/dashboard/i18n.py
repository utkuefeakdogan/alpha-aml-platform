"""Dashboard internationalization — EN (default), DE, TR."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

LOCALES = ("en", "de", "tr")
_LOCALE_DIR = Path(__file__).resolve().parent / "locales"
_CACHE: dict[str, dict[str, str]] = {}


def _load_locale(code: str) -> dict[str, str]:
    if code not in _CACHE:
        path = _LOCALE_DIR / f"{code}.json"
        with open(path, encoding="utf-8") as f:
            _CACHE[code] = json.load(f)
    return _CACHE[code]


def get_locale() -> str:
    loc = st.session_state.get("locale", "en")
    return loc if loc in LOCALES else "en"


def locale_label(code: str) -> str:
    return {"en": "English", "de": "Deutsch", "tr": "Türkçe"}.get(code, code)


def t(key: str, **kwargs) -> str:
    loc = get_locale()
    bundle = _load_locale(loc)
    text = bundle.get(key) or _load_locale("en").get(key) or key
    if kwargs:
        try:
            return text.format(**kwargs)
        except (KeyError, ValueError):
            return text
    return text


def render_language_selector() -> None:
    if "locale" not in st.session_state:
        st.session_state.locale = "en"

    st.sidebar.selectbox(
        t("common.language"),
        LOCALES,
        format_func=locale_label,
        key="locale",
    )
