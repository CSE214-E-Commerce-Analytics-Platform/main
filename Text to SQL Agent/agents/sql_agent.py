from langchain_openai import ChatOpenAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

SCHEMA = """
USERS (is_active, id, created_at, updated_at, email, password_hash, role_type, gender)
REFRESH_TOKENS (revoked_at, replaced_by, created_at, updated_at, id, user_id, expires_at, token)
CATEGORIES (id, parent_id, created_at, updated_at, name)
VERIFICATION_TOKENS (id, user_id, expires_at, used_at, created_at, updated_at, token_type, token)
PRODUCTS (id, unit_price, stock_quantity, store_id, category_id, created_at, updated_at, name, description, image_url, sku)
STORES (id, owner_id, created_at, updated_at, name, status)
"""

def build_role_context(user_role: str, user_id: int, store_id: int) -> str:
    if user_role == "INDIVIDUAL":
        return f"ZORUNLU: Tüm sorgularda WHERE user_id = {user_id} kullan."
    elif user_role == "CORPORATE":
        return f"ZORUNLU: Tüm sorgularda WHERE store_id = {store_id} kullan."
    else:
        return "Admin: tüm verilere erişebilirsin."

def sql_agent(state: AgentState) -> AgentState:
    role_context = build_role_context(
        state["user_role"],
        state["user_id"],
        state.get("store_id")
    )
    
    prompt = f"""Sen bir SQL uzmanısın. Sadece geçerli PostgreSQL SELECT sorgusu yaz.
Açıklama, markdown, kod bloğu kullanma. Sadece ham SQL yaz.

VERİTABANI ŞEMASI:
{SCHEMA}

GÜVENLİK KURALLARI:
{role_context}
- Asla DROP, DELETE, INSERT, UPDATE, ALTER yazma
- Asla password_hash, api_key, internal_cost kolonlarını seçme

Soru: {state['question']}"""

    response = llm.invoke([{"role": "user", "content": prompt}])
    state["sql_query"] = response.content.strip()
    return state