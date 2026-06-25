"""Dark-mode CSS for Alpha AML Compliance Dashboard.

Colors, spacing and radii are centralized as CSS custom properties in :root so
the whole theme can be re-tuned from one place (and a light theme added later).
"""

DARK_THEME_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

    :root {
        /* Backgrounds */
        --bg-grad-top: #0b1220;
        --bg-grad-bottom: #111827;
        --panel: #1e293b;
        --panel-alt: #0f172a;
        --border: #334155;
        /* Accents */
        --accent: #38bdf8;
        --accent-soft: #7dd3fc;
        --accent-strong: #1d4ed8;
        --accent-strong-hover: #2563eb;
        /* Text */
        --text: #f1f5f9;
        --text-strong: #f8fafc;
        --text-body: #cbd5e1;
        --text-muted: #94a3b8;
        --text-faint: #64748b;
        /* Status */
        --ok: #86efac;
        --danger: #fca5a5;
        --success-border: #22c55e;
        /* Shape + spacing scale */
        --radius-sm: 6px;
        --radius-md: 8px;
        --radius-lg: 10px;
        --radius-xl: 12px;
        --pad-tight: 0.65rem 0.75rem;
        --pad-card: 1rem 1.25rem;
        --shadow-card: 0 4px 12px rgba(0, 0, 0, 0.25);
        --shadow-hero: 0 8px 28px rgba(0, 0, 0, 0.35);
        /* Fonts */
        --font-sans: 'IBM Plex Sans', sans-serif;
        --font-mono: 'IBM Plex Mono', 'Consolas', monospace;
    }

    html, body, [class*="css"] {
        font-family: var(--font-sans);
    }

    .stApp {
        background: linear-gradient(180deg, var(--bg-grad-top) 0%, var(--bg-grad-bottom) 100%);
        color: #e5e7eb;
    }

    [data-testid="stSidebar"] {
        background-color: var(--panel-alt);
        border-right: 1px solid #1f2937;
    }

    [data-testid="stSidebar"] * {
        color: var(--text-body) !important;
    }

    h1, h2, h3, h4 {
        color: var(--text-strong) !important;
        font-weight: 600 !important;
    }

    .aml-metric-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: var(--pad-card);
        min-height: 96px;
        box-shadow: var(--shadow-card);
    }

    .aml-metric-label {
        color: var(--text-muted);
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 0.35rem;
    }

    .aml-metric-value {
        color: var(--text);
        font-size: 1.75rem;
        font-weight: 600;
        line-height: 1.2;
    }

    .aml-metric-delta {
        color: var(--accent);
        font-size: 0.8rem;
        margin-top: 0.25rem;
    }

    .aml-footer {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.75rem 1rem;
        margin-top: 1.5rem;
        color: var(--text-muted);
        font-size: 0.85rem;
    }

    .aml-badge-high {
        color: var(--danger);
        font-weight: 600;
    }

    .aml-badge-ok {
        color: var(--ok);
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        overflow: hidden;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        background-color: var(--panel);
        border-radius: var(--radius-md) var(--radius-md) 0 0;
        border: 1px solid var(--border);
        color: var(--text-body);
        padding: 0.5rem 1rem;
    }

    .stTabs [aria-selected="true"] {
        background-color: var(--panel-alt) !important;
        color: var(--accent) !important;
        border-bottom: 2px solid var(--accent) !important;
    }

    .stButton > button {
        background-color: var(--accent-strong);
        color: white;
        border: none;
        border-radius: var(--radius-sm);
        font-weight: 500;
    }

    .stButton > button:hover {
        background-color: var(--accent-strong-hover);
        color: white;
    }

    .aml-alert-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-left: 3px solid var(--accent);
        border-radius: var(--radius-md);
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
    }

    .aml-alert-meta {
        color: var(--text-muted);
        font-size: 0.78rem;
        margin-bottom: 0.35rem;
    }

    .aml-alert-rule {
        color: var(--accent);
        font-weight: 600;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }

    .aml-alert-amount {
        color: var(--text);
        font-size: 1.05rem;
        font-weight: 600;
    }

    .aml-alert-party {
        color: var(--text-body);
        font-size: 0.82rem;
        margin-top: 0.2rem;
    }

    .aml-alert-detail {
        color: var(--text-faint);
        font-size: 0.78rem;
        margin-top: 0.25rem;
        font-style: italic;
    }

    .aml-investigation-panel {
        background: var(--panel-alt);
        border: 1px solid var(--accent);
        border-radius: var(--radius-lg);
        padding: 1.25rem;
        margin: 1rem 0;
    }

    .aml-investigation-title {
        color: var(--accent);
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 0.75rem;
    }

    /* Live feed transaction table — horizontal scroll for full columns */
    div[data-testid="stDataFrame"] > div {
        overflow-x: auto !important;
    }

    /* ---------- Onboarding / Overview page ---------- */
    .ob-hero {
        background: linear-gradient(135deg, var(--panel-alt) 0%, #1e3a8a 100%);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 2.25rem 2.5rem;
        margin-bottom: 1.5rem;
        box-shadow: var(--shadow-hero);
    }
    .ob-hero-badge {
        display: inline-block;
        background: rgba(56,189,248,0.15);
        color: var(--accent-soft);
        border: 1px solid var(--accent);
        border-radius: 999px;
        padding: 0.2rem 0.85rem;
        font-size: 0.75rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.9rem;
    }
    .ob-hero-title {
        color: var(--text-strong);
        font-size: 2.1rem;
        font-weight: 600;
        line-height: 1.15;
        margin: 0 0 0.6rem 0;
    }
    .ob-hero-sub {
        color: var(--text-body);
        font-size: 1.05rem;
        line-height: 1.6;
        max-width: 60rem;
    }
    .ob-section-title {
        color: var(--text-strong);
        font-size: 1.35rem;
        font-weight: 600;
        margin: 1.8rem 0 0.4rem 0;
    }
    .ob-section-sub {
        color: var(--text-muted);
        font-size: 0.95rem;
        margin-bottom: 1rem;
    }
    .ob-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        padding: 1.1rem 1.25rem;
        height: 100%;
        box-shadow: var(--shadow-card);
    }
    .ob-step {
        background: var(--panel);
        border: 1px solid var(--border);
        border-left: 4px solid var(--accent);
        border-radius: var(--radius-lg);
        padding: 1rem 1.15rem;
        height: 100%;
    }
    .ob-step-num {
        color: var(--accent);
        font-size: 0.8rem;
        font-weight: 700;
        letter-spacing: 0.1em;
    }
    .ob-card-icon { font-size: 1.6rem; }
    .ob-card-title {
        color: var(--text);
        font-size: 1.02rem;
        font-weight: 600;
        margin: 0.35rem 0 0.3rem 0;
    }
    .ob-card-body {
        color: #b6c2d2;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    .ob-tag {
        color: var(--accent-soft);
        font-size: 0.78rem;
        font-weight: 500;
        margin-top: 0.5rem;
        font-family: var(--font-mono);
    }
    .ob-tech {
        display: inline-block;
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.45rem 0.8rem;
        margin: 0.25rem;
        color: #e2e8f0;
        font-size: 0.85rem;
    }
    .ob-tech b { color: var(--accent-soft); }

    /* ---------- Overview: engineering fact chips ---------- */
    .ob-fact {
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-left: 3px solid var(--accent);
        border-radius: var(--radius-md);
        padding: 0.7rem 0.95rem;
        height: 100%;
    }
    .ob-fact-val {
        color: var(--accent-soft);
        font-size: 1.3rem;
        font-weight: 600;
        font-family: var(--font-mono);
        line-height: 1.1;
    }
    .ob-fact-label {
        color: var(--text-muted);
        font-size: 0.82rem;
        margin-top: 0.25rem;
    }

    /* ---------- Overview: architecture flow diagram ---------- */
    .arch-flow {
        display: flex;
        flex-wrap: wrap;
        align-items: stretch;
        gap: 0.35rem;
        margin: 0.4rem 0 0.6rem 0;
    }
    .arch-node {
        flex: 1 1 0;
        min-width: 120px;
        background: var(--panel);
        border: 1px solid var(--border);
        border-top: 3px solid var(--accent);
        border-radius: var(--radius-md);
        padding: 0.7rem 0.8rem;
        text-align: center;
    }
    .arch-node-icon { font-size: 1.35rem; }
    .arch-node-tech {
        color: var(--text-strong);
        font-weight: 600;
        font-size: 0.9rem;
        margin: 0.25rem 0 0.15rem 0;
    }
    .arch-node-role {
        color: var(--text-muted);
        font-size: 0.74rem;
        line-height: 1.35;
    }
    .arch-arrow {
        align-self: center;
        color: var(--accent);
        font-size: 1.2rem;
        font-weight: 700;
        padding: 0 0.1rem;
    }
    .arch-sidecar {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-top: 0.2rem;
    }
    .arch-sidecar-item {
        flex: 1 1 0;
        min-width: 220px;
        background: var(--panel-alt);
        border: 1px dashed var(--border);
        border-radius: var(--radius-md);
        padding: 0.6rem 0.85rem;
        color: var(--text-body);
        font-size: 0.84rem;
    }
    .arch-sidecar-item b { color: var(--accent-soft); }

    /* ---------- Overview: medallion lanes ---------- */
    .medallion {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 0.95rem 1.1rem;
        height: 100%;
    }
    .medallion-bronze { border-top: 4px solid #b45309; }
    .medallion-silver { border-top: 4px solid #94a3b8; }
    .medallion-gold   { border-top: 4px solid #d4a017; }
    .medallion-tier {
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: var(--text-muted);
    }
    .medallion-title {
        color: var(--text-strong);
        font-size: 1.0rem;
        font-weight: 600;
        margin: 0.15rem 0 0.5rem 0;
    }
    .medallion-tbl {
        display: block;
        font-family: var(--font-mono);
        font-size: 0.78rem;
        color: #e2e8f0;
        padding: 0.18rem 0;
        border-bottom: 1px solid rgba(51,65,85,0.5);
    }
    .medallion-note {
        color: var(--text-faint);
        font-size: 0.76rem;
        margin-top: 0.55rem;
        line-height: 1.45;
    }

    /* ---------- Overview: layered tech stack ---------- */
    .layer-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.55rem 0.85rem;
        margin-bottom: 0.4rem;
    }
    .layer-name {
        flex: 0 0 170px;
        color: var(--accent-soft);
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .layer-tools { color: var(--text-body); font-size: 0.88rem; }
    .layer-tools b { color: var(--text-strong); }

    /* ---------- SQL Explorer (IDE-style) ---------- */
    .sql-ide-banner {
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-left: 4px solid var(--success-border);
        border-radius: var(--radius-lg);
        padding: var(--pad-card);
        margin-bottom: 1rem;
    }
    .sql-ide-banner-title {
        color: var(--ok);
        font-weight: 600;
        font-size: 0.95rem;
        margin-bottom: 0.5rem;
    }
    .sql-ide-banner-body {
        color: var(--text-muted);
        font-size: 0.88rem;
        line-height: 1.55;
    }
    .sql-ide-banner-body a { color: var(--accent); text-decoration: none; }
    .sql-ide-banner-body a:hover { text-decoration: underline; }
    .sql-ide-panel {
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: var(--pad-tight);
    }
    .sql-ide-schema-panel {
        max-height: 380px;
        overflow-y: auto;
    }
    .sql-ide-examples-wrap {
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 0.55rem 0.75rem 0.35rem;
        margin-bottom: 0.65rem;
    }
    .sql-ide-schema-badge {
        display: inline-block;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 999px;
        padding: 0.15rem 0.55rem;
        font-size: 0.72rem;
        color: var(--text-muted);
        margin-bottom: 0.45rem;
    }
    .sql-ide-panel-title {
        color: var(--accent-soft);
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.75rem;
    }
    .sql-ide-table-pill {
        display: block;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        padding: 0.35rem 0.6rem;
        margin-bottom: 0.35rem;
        color: #e2e8f0;
        font-family: var(--font-mono);
        font-size: 0.78rem;
    }
    .sql-ide-table-pill:hover { border-color: var(--accent); }
    .sql-ide-user-badge {
        display: inline-block;
        background: #172554;
        border: 1px solid var(--accent-strong);
        color: #93c5fd;
        border-radius: var(--radius-sm);
        padding: 0.15rem 0.55rem;
        font-family: var(--font-mono);
        font-size: 0.82rem;
    }

    /* ---------- Scenarios showcase cards ---------- */
    .scn-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-top: 3px solid var(--accent);
        border-radius: var(--radius-md);
        padding: 0.85rem 1rem 0.95rem 1rem;
        margin-bottom: 0.85rem;
    }
    .scn-head {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.45rem;
        margin-bottom: 0.35rem;
    }
    .scn-title {
        color: var(--text-strong);
        font-weight: 600;
        font-size: 1rem;
        margin-right: auto;
    }
    .scn-badge {
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        padding: 0.12rem 0.5rem;
        border-radius: 999px;
        white-space: nowrap;
    }
    .scn-typ {
        background: var(--panel-alt);
        color: var(--accent-soft);
        border: 1px solid var(--border);
    }
    .scn-on { background: rgba(34, 197, 94, 0.14); color: #16a34a; border: 1px solid rgba(34,197,94,0.35); }
    .scn-off { background: rgba(148, 163, 184, 0.14); color: var(--text-muted); border: 1px solid var(--border); }
    .scn-desc { color: var(--text-body); font-size: 0.86rem; line-height: 1.4; }
    .scn-detect {
        color: var(--text-strong);
        font-size: 0.84rem;
        background: var(--panel-alt);
        border-left: 3px solid var(--accent);
        border-radius: var(--radius-sm);
        padding: 0.45rem 0.6rem;
        margin: 0.5rem 0;
    }
    .scn-meta { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-bottom: 0.5rem; }
    .scn-chip {
        font-size: 0.74rem;
        color: var(--text-muted);
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        padding: 0.18rem 0.5rem;
    }
    .scn-params {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 0.4rem;
    }
    .scn-param {
        display: flex;
        flex-direction: column;
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        padding: 0.4rem 0.55rem;
    }
    .scn-pk { color: var(--text-muted); font-size: 0.72rem; }
    .scn-pv { color: var(--accent-soft); font-size: 0.92rem; font-weight: 600; font-family: var(--font-mono); }

    /* ---------- System Health ---------- */
    .hc-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-left: 4px solid var(--text-muted);
        border-radius: var(--radius-md);
        padding: 0.7rem 0.85rem;
        height: 100%;
    }
    .hc-card.hc-ok { border-left-color: #16a34a; }
    .hc-card.hc-stale { border-left-color: #d4a017; }
    .hc-card.hc-down { border-left-color: #dc2626; }
    .hc-head { display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.45rem; }
    .hc-dot { width: 9px; height: 9px; border-radius: 999px; flex: 0 0 auto; }
    .hc-dot-ok { background: #16a34a; box-shadow: 0 0 0 3px rgba(22,163,74,0.18); }
    .hc-dot-stale { background: #d4a017; box-shadow: 0 0 0 3px rgba(212,160,23,0.18); }
    .hc-dot-down { background: #dc2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.18); }
    .hc-name {
        color: var(--text-strong);
        font-size: 0.84rem;
        font-weight: 600;
        margin-right: auto;
    }
    .hc-pill {
        font-size: 0.62rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        padding: 0.1rem 0.4rem;
        border-radius: 999px;
        white-space: nowrap;
    }
    .hc-pill-ok { background: rgba(22,163,74,0.14); color: #16a34a; }
    .hc-pill-stale { background: rgba(212,160,23,0.16); color: #d4a017; }
    .hc-pill-down { background: rgba(220,38,38,0.16); color: #f87171; }
    .hc-val {
        color: var(--accent-soft);
        font-size: 1.05rem;
        font-weight: 600;
        font-family: var(--font-mono);
        line-height: 1.2;
    }
    .hc-sub { color: var(--text-muted); font-size: 0.74rem; margin-top: 0.2rem; }
    .hc-check {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        background: var(--panel-alt);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        padding: 0.45rem 0.7rem;
        margin-bottom: 0.4rem;
    }
    .hc-check-icon {
        font-weight: 700;
        width: 1.1rem;
        text-align: center;
        flex: 0 0 auto;
    }
    .hc-check-ok .hc-check-icon { color: #16a34a; }
    .hc-check-down .hc-check-icon { color: #f87171; }
    .hc-check-label { color: var(--text-body); font-size: 0.86rem; margin-right: auto; }
    .hc-check-detail {
        color: var(--text-muted);
        font-size: 0.8rem;
        font-family: var(--font-mono);
    }
</style>
"""
