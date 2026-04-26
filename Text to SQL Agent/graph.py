import re
from langgraph.graph import StateGraph, END
from state import AgentState
from agents.guardrails_agent import guardrails_agent
from agents.sql_agent import sql_agent
from agents.error_agent import error_agent
from agents.analysis_agent import analysis_agent
from agents.visualization_agent import visualization_agent
from tools.db_executor import execute_query

BLOCKED_SIGNALS = {"READ_ONLY_PERMISSIONS", "INTROSPECTION_DETECTED", "HIGH_TRAFFIC_WARNING", "MISSING_DATA_TABLE", "UNFIXABLE", "SCOPE_VIOLATION", "INJECTION_DETECTED"}

# ── Blocked-signal canned messages (bilingual) ────────────────────────────────
_BLOCKED_MESSAGES = {
    "en": {
        "MISSING_DATA_TABLE": "There is currently no data available for that information.",
        "READ_ONLY_PERMISSIONS": "My current permissions are limited to Read-Only analytics.",
        "INTROSPECTION_DETECTED": "I am an AI assistant designed to provide E-commerce Analytics. I can help you query orders, products, and users based on your role permissions.",
        "HIGH_TRAFFIC_WARNING": "High Traffic warning: Automated sequential ID scraping detected. Please provide specific queries instead.",
        "SCOPE_VIOLATION":   "You do not have permission to view this data.",
        "INJECTION_DETECTED": "Your request was blocked for security reasons.",
        "UNFIXABLE":         "This query could not be processed. Please try rephrasing your question.",
    },
    "tr": {
        "MISSING_DATA_TABLE": "Bu bilgi için platformda henüz veri bulunmamaktadır.",
        "READ_ONLY_PERMISSIONS": "Mevcut izinlerim yalnızca Salt Okunur analizlerle sınırlıdır.",
        "INTROSPECTION_DETECTED": "Ben e-ticaret analitiği sağlamak için tasarlanmış bir yapay zeka asistanıyım. Rol izinlerinize dayanarak siparişleri, ürünleri ve kullanıcıları sorgulamanıza yardımcı olabilirim.",
        "HIGH_TRAFFIC_WARNING": "Yüksek Trafik uyarısı: Otomatik ardışık ID taraması tespit edildi. Lütfen bunun yerine belirli sorgular sağlayın.",
        "SCOPE_VIOLATION":   "Bu verileri görüntüleme yetkiniz bulunmamaktadır.",
        "INJECTION_DETECTED": "İsteğiniz güvenlik nedeniyle engellendi.",
        "UNFIXABLE":         "Bu sorgu işlenemedi. Lütfen soruyu farklı bir şekilde sormayı deneyin.",
    },
}


def _categorize_error(error_str: str) -> str:
    """Classify a raw DB error string into an actionable bucket for error_agent."""
    msg = error_str.lower()
    if "column" in msg and "does not exist" in msg:
        return "column_missing"
    if ("relation" in msg or "table" in msg) and "does not exist" in msg:
        return "table_missing"
    if "syntax error" in msg:
        return "syntax_error"
    if "ambiguous" in msg:
        return "ambiguous_column"
    if "permission denied" in msg:
        return "permission"
    if "division by zero" in msg:
        return "division_by_zero"
    return "other"

def execute_sql_node(state: AgentState) -> AgentState:
    raw_sql    = state.get("sql_query", "") or ""
    signal     = raw_sql.strip().upper()
    language   = state.get("language") or "en"
    lang_msgs  = _BLOCKED_MESSAGES.get(language, _BLOCKED_MESSAGES["en"])

    print(f"\n{'─'*52}")
    print(f"[ExecuteSQL] sql_preview={raw_sql[:120]!r}")
    print(f"{'─'*52}\n")

    trace = state.get("trace", []) + ["DBExecutor"]

    # ── Short-circuit: blocked signal — never hits the DB ────────────────────
    if signal in BLOCKED_SIGNALS:
        message = lang_msgs.get(signal, lang_msgs["UNFIXABLE"])
        print(f"[ExecuteSQL] Blocked signal: {signal!r}")
        return {
            **state,
            "query_result": None,
            "error": None,
            "sql_error_type": signal,
            "final_answer": message,
            "trace": trace,
        }

    # ── Execute ───────────────────────────────────────────────────────────────
    result = execute_query(
        raw_sql,
        state["user_role"],
        state["user_id"],
        state.get("store_id"),
    )

    if result.startswith("DB_ERROR") or result.startswith("ERROR"):
        error_type = _categorize_error(result)
        print(f"[ExecuteSQL] DB error type={error_type!r} | {result[:120]!r}")
        return {
            **state,
            "query_result": None,
            "error": result,
            "sql_error_type": error_type,
            "trace": trace,
        }

    print(f"[ExecuteSQL] Success — result_len={len(result)}")
    return {
        **state,
        "query_result": result,
        "error": None,
        "sql_error_type": None,
        "trace": trace,
    }

# --- Karar fonksiyonları ---

def should_continue(state: AgentState) -> str:
    if not state.get("is_in_scope"):
        return "end"
    # If guardrails already set a final_answer (e.g. greeting), short-circuit
    if state.get("final_answer"):
        return "end"
    return "sql"

def check_error(state: AgentState) -> str:
    # Blocked signal or greeting already set final_answer — skip to end
    if state.get("final_answer"):
        return "end"
    if state.get("error"):
        iteration  = state.get("iteration_count", 0)
        error_type = state.get("sql_error_type", "other")
        if iteration >= 3 or error_type in ("permission", "rbac_violation"):
            # Max retries hit or unrecoverable error type — friendly fallback
            language  = state.get("language") or "en"
            if language == "tr":
                fallback = "Bu sorgu şu an işlenemiyor. Lütfen soruyu farklı bir şekilde sormayı deneyin."
            else:
                fallback = "This query cannot be processed at the moment. Please try rephrasing your question."
            # Mutate-in-place is fine here — this is the terminal branch
            state["final_answer"] = fallback
            return "end"
        return "error"
    return "analysis"

# --- Graph ---

def build_graph():
    graph = StateGraph(AgentState)

    # Node'ları ekle
    graph.add_node("guardrails", guardrails_agent)
    graph.add_node("sql", sql_agent)
    graph.add_node("execute", execute_sql_node)
    graph.add_node("error", error_agent)
    graph.add_node("analysis", analysis_agent)
    graph.add_node("visualization", visualization_agent)

    # Başlangıç
    graph.set_entry_point("guardrails")

    # Guardrails → greeting/out_of_scope END'e, in_scope sql'e
    graph.add_conditional_edges(
        "guardrails",
        should_continue,
        {
            "end": END,
            "sql": "sql"
        }
    )

    # sql → execute
    graph.add_edge("sql", "execute")

    # execute → hata varsa error, başarılıysa analysis
    graph.add_conditional_edges(
        "execute",
        check_error,
        {
            "error": "error",
            "analysis": "analysis",
            "end": END
        }
    )

    # error → tekrar execute (retry loop)
    graph.add_edge("error", "execute")

    # analysis → decide_graph_need
    graph.add_edge("analysis", "visualization")

    # visualization → END
    graph.add_edge("visualization", END)

    return graph.compile()