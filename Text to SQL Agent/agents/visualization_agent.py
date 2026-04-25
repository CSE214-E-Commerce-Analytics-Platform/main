import ast
import json
import urllib.parse
from state import AgentState
from dotenv import load_dotenv

load_dotenv()

# ── Keyword sets for chart-type heuristics ────────────────────────────────────

_DATE_KEYWORDS     = ("date", "day", "month", "week", "year", "_at", "time", "period", "quarter")
_RANKING_KEYWORDS  = ("top", "rank", "most", "least", "best", "worst", "highest", "lowest", "en çok", "en az", "en iyi", "en kötü")
_TREND_KEYWORDS    = ("trend", "over time", "overtime", "growth", "change", "zaman", "gelişim", "artış", "düşüş")
_COMPARE_KEYWORDS  = ("compare", "vs", "versus", "karşılaştır", "karşılaştırma", "radar")

# Chart.js color palette — used for bar and pie charts
_PALETTE = [
    "rgba(99,102,241,0.85)",   # indigo
    "rgba(16,185,129,0.85)",   # emerald
    "rgba(245,158,11,0.85)",   # amber
    "rgba(239,68,68,0.85)",    # red
    "rgba(59,130,246,0.85)",   # blue
    "rgba(168,85,247,0.85)",   # purple
    "rgba(20,184,166,0.85)",   # teal
    "rgba(251,191,36,0.85)",   # yellow
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_numeric(val) -> bool:
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def _is_date_col(col_name: str) -> bool:
    return any(kw in col_name.lower() for kw in _DATE_KEYWORDS)


def _parse_query_result(raw: str) -> tuple[list[str], list[list]]:
    """Parse the string returned by db_executor into (columns, rows).
    Returns ([], []) if the result is empty or unparseable.
    """
    if not raw or raw == "No results found.":
        return [], []
    try:
        records: list[dict] = ast.literal_eval(raw)
        if not records:
            return [], []
        columns = list(records[0].keys())
        rows    = [[r.get(c) for c in columns] for r in records]
        return columns, rows
    except Exception:
        return [], []


# ── Chart type decision ───────────────────────────────────────────────────────

def decide_chart(columns: list[str], rows: list[list], question: str) -> str:
    """Return a Chart.js chart type string, or 'none' if no chart is appropriate."""
    n_rows = len(rows)
    q      = question.lower()

    if n_rows == 0:
        return "none"
    if n_rows == 1 and len(columns) == 1:
        return "none"  # single scalar — text answer is enough

    # Classify columns by value type
    sample_row   = rows[0]
    numeric_cols = [c for c, v in zip(columns, sample_row) if _is_numeric(v)]
    date_cols    = [c for c in columns if _is_date_col(c)]
    n_numeric    = len(numeric_cols)

    # Time-series: date column + at least one numeric
    if (date_cols or any(kw in q for kw in _TREND_KEYWORDS)) and n_numeric >= 1:
        return "line"

    # Radar: specific keywords for feature comparison
    if "radar" in q or "spider" in q:
        return "radar"

    # Proportion / share: non-numeric label + single numeric + few slices
    non_numeric = [c for c in columns if c not in numeric_cols]
    if non_numeric and n_numeric == 1 and 2 <= n_rows <= 10:
        return "doughnut"

    # Determine if we should use horizontal bar (long labels)
    use_horizontal = False
    if non_numeric:
        avg_len = sum(len(str(row[columns.index(non_numeric[0])])) for row in rows) / max(n_rows, 1)
        if avg_len > 12:
            use_horizontal = True

    # Ranking or comparison
    if (any(kw in q for kw in _RANKING_KEYWORDS) or any(kw in q for kw in _COMPARE_KEYWORDS)) and n_rows <= 25:
        return "horizontalBar" if use_horizontal else "bar"

    # General multi-row result with a numeric column → bar
    if n_rows > 0 and n_numeric >= 1:
        return "horizontalBar" if use_horizontal else "bar"

    return "none"


# ── Column picker ─────────────────────────────────────────────────────────────

def _pick_columns(columns: list[str], rows: list[list], chart_type: str) -> tuple[str, str]:
    """Return (x_col, y_col) best suited for the chosen chart type.

    ID/FK columns (id, user_id, store_id, …) are row identifiers — they must
    never be the Y axis. Prefer meaningful metric columns like stock_quantity,
    grand_total, unit_price, star_rating, count, etc.
    """
    # Column names that are identifiers, not metrics
    _ID_SUFFIXES = ("_id", "_at")
    _ID_EXACT    = {"id"}

    def _is_id_col(col: str) -> bool:
        c = col.lower()
        return c in _ID_EXACT or any(c.endswith(s) for s in _ID_SUFFIXES)

    sample       = rows[0] if rows else []
    numeric_cols = [c for c, v in zip(columns, sample) if _is_numeric(v)]
    non_numeric  = [c for c in columns if c not in numeric_cols]
    date_cols    = [c for c in columns if _is_date_col(c)]

    # Metric columns: numeric but NOT an id/fk/timestamp column
    metric_cols  = [c for c in numeric_cols if not _is_id_col(c)]
    # Fall back to all numeric if no pure metric column exists
    y_candidates = metric_cols if metric_cols else numeric_cols

    if chart_type == "line":
        x = date_cols[0] if date_cols else (non_numeric[0] if non_numeric else columns[0])
        y = y_candidates[0] if y_candidates else columns[-1]
    elif chart_type in ("bar", "horizontalBar", "pie", "doughnut", "radar"):
        x = non_numeric[0] if non_numeric else columns[0]
        y = y_candidates[0] if y_candidates else columns[-1]
    else:
        x = columns[0]
        y = columns[-1] if len(columns) > 1 else columns[0]

    return x, y


# ── Chart.js config builder ───────────────────────────────────────────────────

def _build_chartjs_config(
    columns: list[str],
    rows: list[list],
    chart_type: str,
    x_col: str,
    y_col: str,
    title: str,
) -> dict:
    """Build a Chart.js configuration dict ready for QuickChart.io."""
    col_idx = {c: i for i, c in enumerate(columns)}
    x_idx   = col_idx.get(x_col, 0)
    y_idx   = col_idx.get(y_col, len(columns) - 1)

    x_vals  = [str(row[x_idx]) for row in rows]
    y_vals  = [row[y_idx] for row in rows]

    colors  = [_PALETTE[i % len(_PALETTE)] for i in range(len(rows))]

    is_radial = chart_type in ("pie", "doughnut")
    is_horizontal = chart_type == "horizontalBar"
    actual_type = "bar" if is_horizontal else chart_type

    if is_radial:
        dataset = {
            "data":            y_vals,
            "backgroundColor": colors,
        }
    else:
        dataset = {
            "label":           y_col,
            "data":            y_vals,
            "backgroundColor": colors[0] if actual_type in ("line", "radar") else colors,
            "borderColor":     colors[0],
            "fill":            True if actual_type == "radar" else False,
            "tension":         0.3,
        }

    options = {
        "plugins": {
            "title": {
                "display": True,
                "text":    title,
                "font":    {"size": 14},
            }
        }
    }

    if is_horizontal:
        options["indexAxis"] = "y"

    if not is_radial and actual_type != "radar":
        options["scales"] = {
            "y": {"beginAtZero": True} if not is_horizontal else {},
            "x": {"beginAtZero": True} if is_horizontal else {}
        }

    config = {
        "type": actual_type,
        "data": {
            "labels":   x_vals,
            "datasets": [dataset],
        },
        "options": options,
    }

    return config


def _build_chart_url(config: dict, width: int = 600, height: int = 350) -> str:
    """Encode a Chart.js config as a QuickChart.io image URL."""
    chart_json  = json.dumps(config, ensure_ascii=False)
    encoded     = urllib.parse.quote(chart_json)
    # Use Chart.js v3 to support 'plugins.title' and use a larger canvas for retina-like scaling
    return f"https://quickchart.io/chart?v=3&c={encoded}&w={width}&h={height}&bkg=white"


# ── Main agent ────────────────────────────────────────────────────────────────

def visualization_agent(state: AgentState) -> AgentState:
    question    = state["question"]
    raw_result  = state.get("query_result") or ""
    final       = state.get("final_answer") or ""
    language    = state.get("language") or "en"

    # ── 1. Parse result ───────────────────────────────────────────────────────
    columns, rows = _parse_query_result(raw_result)

    trace = state.get("trace", []) + ["VisualizationAgent"]

    if not columns:
        # No parseable data — nothing to visualise
        return {**state, "trace": trace}

    # ── 2. Decide chart type ──────────────────────────────────────────────────
    chart_type = decide_chart(columns, rows, question)
    print(f"[Visualization] chart_type={chart_type!r} rows={len(rows)} cols={columns}")

    if chart_type == "none":
        return {**state, "trace": trace}

    # ── 3. Pick columns + build config ───────────────────────────────────────
    x_col, y_col = _pick_columns(columns, rows, chart_type)
    title        = question[:60].strip().rstrip("?") + "?"

    try:
        config    = _build_chartjs_config(columns, rows, chart_type, x_col, y_col, title)
        chart_url = _build_chart_url(config)

        # ── 4. Append chart image to final_answer ──────────────────────────
        chart_line  = f"\n\n![Chart]({chart_url})"

        return {
            **state,
            "final_answer":      final + chart_line,
            "visualization_code": json.dumps(config, ensure_ascii=False),
            "trace": trace,
        }

    except Exception as e:
        print(f"[Visualization] Chart build failed: {e}")
        return {**state, "trace": trace}