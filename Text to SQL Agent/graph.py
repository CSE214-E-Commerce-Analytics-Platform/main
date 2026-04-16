from langgraph.graph import StateGraph, END
from state import AgentState
from agents.guardrails_agent import guardrails_agent
from agents.sql_agent import sql_agent
from agents.error_agent import error_agent
from agents.analysis_agent import analysis_agent
from agents.visualization_agent import visualization_agent
from tools.db_executor import execute_query

BLOCKED_SIGNALS = {"SCOPE_VIOLATION", "INJECTION_DETECTED", "UNFIXABLE", "MISSING_DATA_TABLE"}

def execute_sql_node(state: AgentState) -> AgentState:
    sql = state.get("sql_query", "").strip().upper()

    print("\n" + "-"*50)
    print(f"[SQL AGENT] Veritabanına Gönderilecek SQL:")
    print(f"{state.get('sql_query')}")
    print("-"*50 + "\n")

    # Short-circuit: blocked signal from SQL/error agent — do not hit the DB
    if sql == "MISSING_DATA_TABLE":
        state["query_result"] = None
        state["final_answer"] = "There is currently no table on the platform containing this information."
        return state
    elif sql == "SCOPE_VIOLATION":
        state["query_result"] = None
        state["final_answer"] = "[AUTHORIZATION ERROR] SQL Agent: You do not have permission to view this data, or it does not belong to you."
        return state
    elif sql == "INJECTION_DETECTED":
        state["query_result"] = None
        state["final_answer"] = "[SECURITY] SQL Agent: Malicious command detected."
        return state
    elif sql == "UNFIXABLE":
        state["query_result"] = None
        state["final_answer"] = "[SYSTEM] SQL Agent: Your query cannot be processed at this time."
        return state

    result = execute_query(
        state["sql_query"],
        state["user_role"],
        state["user_id"],
        state.get("store_id")
    )
    if result.startswith("DB_ERROR") or result.startswith("ERROR"):
        state["error"] = result
        state["query_result"] = None
    else:
        state["query_result"] = result
        state["error"] = None
    return state

# --- Karar fonksiyonları ---

def should_continue(state: AgentState) -> str:
    if not state.get("is_in_scope"):
        return "end"
    # If guardrails already set a final_answer (e.g. greeting), short-circuit
    if state.get("final_answer"):
        return "end"
    return "sql"

def check_error(state: AgentState) -> str:
    # If a blocked signal already set final_answer, go straight to end
    if state.get("final_answer"):
        return "end"
    if state.get("error"):
        if state.get("iteration_count", 0) >= 3:
            # Max retries hit — return a friendly fallback instead of None
            state["final_answer"] = "This query cannot be processed at the moment, please try a different question."
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