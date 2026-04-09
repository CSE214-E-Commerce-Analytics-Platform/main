from langgraph.graph import StateGraph, END
from state import AgentState
from agents.guardrails_agent import guardrails_agent
from agents.sql_agent import sql_agent
from agents.error_agent import error_agent
from agents.analysis_agent import analysis_agent
from agents.visualization_agent import visualization_agent
from tools.db_executor import execute_query

def execute_sql_node(state: AgentState) -> AgentState:
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
    return "sql"

def check_error(state: AgentState) -> str:
    if state.get("error"):
        if state.get("iteration_count", 0) >= 3:
            return "end"
        return "error"
    return "analysis"

def decide_graph_need(state: AgentState) -> str:
    if state.get("visualization_code"):
        return "visualization"
    return "end"

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
    graph.add_conditional_edges(
        "analysis",
        decide_graph_need,
        {
            "visualization": "visualization",
            "end": END
        }
    )

    # visualization → END
    graph.add_edge("visualization", END)

    return graph.compile()