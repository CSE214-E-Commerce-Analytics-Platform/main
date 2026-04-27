import re
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from state import AgentState
from dotenv import load_dotenv

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────

BLOCKED_SIGNALS = {"SCOPE_VIOLATION", "INJECTION_DETECTED", "UNFIXABLE"}

_SYSTEM_PROMPT = """You are a PostgreSQL syntax error correction specialist.

YOUR ONLY TASK:
Fix the error in the SQL query you receive. Do not change its logic, scope, or intent.

CONTEXT:
- User role  : {role}
- Error type : {error_type}
- Scope rule : {scope_rule}

ABSOLUTE RULES:
1. Output the corrected SQL only — no explanation, no markdown, no code blocks.
2. Do NOT expand scope — preserve every WHERE clause exactly as-is.
3. Do NOT add or remove WHERE clauses.
4. Do NOT introduce columns that were not in the original query.
5. Never output DROP, DELETE, INSERT, UPDATE, ALTER under any circumstance.
6. If the input contains SCOPE_VIOLATION or INJECTION_DETECTED → output: UNFIXABLE
7. If the error cannot be resolved with a safe SELECT → output: UNFIXABLE

ALLOWED SCHEMA FOR REFERENCE:
USERS      : id, email, role_type, provider, is_active, created_at, updated_at
PRODUCTS   : id, name, sku, unit_price, stock_quantity, store_id, category_id, description, image_url, created_at, updated_at
STORES     : id, name, status, owner_id, created_at, updated_at
CATEGORIES : id, parent_id, name, created_at, updated_at
ORDERS     : id, status, grand_total, created_at, store_id, user_id
ORDER_ITEMS: id, order_id, product_id, quantity, price
REVIEWS    : id, star_rating, sentiment, product_id, user_id
SHIPMENTS  : id, order_id, warehouse, mode, status

OUTPUT: Fixed SQL only. If unfixable → UNFIXABLE."""

# ── LLM (lazy singleton) ──────────────────────────────────────────────────────

_LLM: ChatOpenAI | None = None


def _get_llm() -> ChatOpenAI:
    global _LLM
    if _LLM is None:
        _LLM = ChatOpenAI(model="gpt-4o", temperature=0)
    return _LLM


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_sql_fences(text: str) -> str:
    """Remove markdown code fences the LLM sometimes wraps around SQL output."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def _classify_error_type(error: str) -> str:
    """Categorise the DB error string so the prompt can be more targeted.
    Returns a short label — never shown to the user."""
    if not error:
        return "unknown"
    err_lower = error.lower()
    if "column" in err_lower and "does not exist" in err_lower:
        return "invalid_column"
    if "relation" in err_lower and "does not exist" in err_lower:
        return "invalid_table"
    if "syntax error" in err_lower:
        return "syntax"
    if "ambiguous" in err_lower:
        return "ambiguous_column"
    if "permission denied" in err_lower or "pg_" in err_lower:
        return "permission"
    if "division by zero" in err_lower:
        return "division_by_zero"
    return "other"


def _build_scope_rule(user_role: str, user_id: int, store_id: int | None) -> str:
    """Return a concise scope reminder so the LLM never widens the WHERE clause."""
    if user_role == "ADMIN":
        return "ADMIN role — full platform access, no WHERE restriction required."
    if user_role == "CORPORATE":
        return f"CORPORATE role — query MUST keep WHERE store_id = {store_id}."
    return f"INDIVIDUAL role — query MUST keep WHERE user_id = {user_id}."


# ── Main agent ────────────────────────────────────────────────────────────────

def error_agent(state: AgentState) -> AgentState:
    sql            = state.get("sql_query", "").strip()
    error_msg      = state.get("error", "") or ""
    iteration      = state.get("iteration_count", 0) + 1
    role           = state.get("user_role", "INDIVIDUAL")
    user_id        = state.get("user_id")
    store_id       = state.get("store_id")

    trace          = state.get("trace", []) + ["ErrorAgent"]

    # ── 1. Short-circuit on blocked signals ───────────────────────────────────
    if sql.upper() in BLOCKED_SIGNALS:
        print(f"[ErrorAgent] Blocked signal detected ({sql.upper()!r}), marking UNFIXABLE.")
        return {
            **state,
            "sql_query": "UNFIXABLE",
            "error": None,
            "iteration_count": iteration,
            "trace": trace,
        }

    # ── 2. Classify error type for prompt context ─────────────────────────────
    error_type  = _classify_error_type(error_msg)
    scope_rule  = _build_scope_rule(role, user_id, store_id)

    print(
        f"[ErrorAgent] attempt={iteration} role={role} "
        f"error_type={error_type!r} | {error_msg[:120]!r}"
    )

    # ── 3. Build messages ─────────────────────────────────────────────────────
    system_prompt = _SYSTEM_PROMPT.format(
        role=role,
        error_type=error_type,
        scope_rule=scope_rule,
    )

    user_content = (
        f"Broken SQL query:\n{sql}\n\n"
        f"Error message:\n{error_msg}"
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_content),
    ]

    # ── 4. Invoke LLM ─────────────────────────────────────────────────────────
    response     = _get_llm().invoke(messages)
    corrected    = _strip_sql_fences(response.content)

    print(f"[ErrorAgent] corrected_sql_preview={corrected[:120]!r}")

    return {
        **state,
        "sql_query": corrected,
        "error": None,          # cleared so execute_sql_node re-runs cleanly
        "iteration_count": iteration,
        "trace": trace,
    }
