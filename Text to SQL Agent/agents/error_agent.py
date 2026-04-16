from langchain_openai import ChatOpenAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

SYSTEM_PROMPT = """You are a PostgreSQL syntax error correction specialist.

YOUR ONLY TASK:
Fix the syntax error in the SQL query you receive. Do not change its logic, scope, or intent.

ABSOLUTE RULES:
1. Output the corrected SQL only. No explanation, no markdown, no code blocks.
2. Do NOT expand scope — if the original query has WHERE store_id = 5, keep it exactly as store_id = 5.
3. Do NOT add or remove WHERE clauses.
4. Do NOT introduce columns that were not in the original query.
5. Never output DROP, DELETE, INSERT, UPDATE, ALTER under any circumstance.
6. If the input contains SCOPE_VIOLATION or INJECTION_DETECTED → output: UNFIXABLE
7. If the error cannot be fixed with a safe SELECT → output: UNFIXABLE

ALLOWED COLUMNS FOR REFERENCE:
USERS: id, email, role_type, gender, is_active, created_at, updated_at
PRODUCTS: id, name, sku, unit_price, stock_quantity, store_id, category_id, description, image_url, created_at, updated_at
STORES: id, name, status, owner_id, created_at, updated_at
CATEGORIES: id, parent_id, name, created_at, updated_at

OUTPUT: Fixed SQL only. If unfixable → UNFIXABLE."""

def error_agent(state: AgentState) -> AgentState:
    # If the SQL itself is a violation signal, mark as unfixable immediately
    blocked_signals = {"SCOPE_VIOLATION", "INJECTION_DETECTED", "UNFIXABLE"}
    if state.get("sql_query", "").strip().upper() in blocked_signals:
        state["sql_query"] = "UNFIXABLE"
        state["error"] = None
        state["iteration_count"] = state.get("iteration_count", 0) + 1
        return state

    user_message = f"""Broken SQL query: {state['sql_query']}
Error message: {state['error']}"""

    response = llm.invoke([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message}
    ])
    state["sql_query"] = response.content.strip()
    state["error"] = None
    state["iteration_count"] = state.get("iteration_count", 0) + 1
    return state