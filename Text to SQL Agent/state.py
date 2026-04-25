from typing import TypedDict, Optional

class AgentState(TypedDict):
    question: str
    user_role: str        # "ADMIN", "CORPORATE", "INDIVIDUAL"
    user_id: int
    store_id: Optional[int]
    sql_query: Optional[str]
    query_result: Optional[str]
    error: Optional[str]
    final_answer: Optional[str]
    visualization_code: Optional[str]
    is_in_scope: Optional[bool]
    iteration_count: int
    trace: list[str]
    suggestions: list[str]
    # Set by guardrails agent
    intent: Optional[str]           # "sql_query" | "greeting" | "off_topic" | "unsafe"
    language: Optional[str]         # "en" | "tr"
    guardrail_reason: Optional[str] # internal reason string (not shown to user)
    # Set by execute_sql_node / error_agent
    sql_error_type: Optional[str]   # "syntax_error" | "column_missing" | "table_missing" | "ambiguous_column" | "permission" | "other"