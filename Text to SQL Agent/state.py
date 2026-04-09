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