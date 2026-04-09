from langchain_google_genai import ChatGoogleGenerativeAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

def error_agent(state: AgentState) -> AgentState:
    prompt = f"""Sen bir SQL hata düzeltme uzmanısın.
Hatalı SQL sorgusunu ve hata mesajını analiz et, düzeltilmiş sorguyu yaz.
Sadece düzeltilmiş SQL yaz, açıklama ekleme.

Şema:
USERS (is_active, id, created_at, updated_at, email, password_hash, role_type, gender)
REFRESH_TOKENS (revoked_at, replaced_by, created_at, updated_at, id, user_id, expires_at, token)
CATEGORIES (id, parent_id, created_at, updated_at, name)
VERIFICATION_TOKENS (id, user_id, expires_at, used_at, created_at, updated_at, token_type, token)
PRODUCTS (id, unit_price, stock_quantity, store_id, category_id, created_at, updated_at, name, description, image_url, sku)
STORES (id, owner_id, created_at, updated_at, name, status)

Hatalı SQL: {state['sql_query']}
Hata: {state['error']}"""

    response = llm.invoke([{"role": "user", "content": prompt}])
    state["sql_query"] = response.content.strip()
    state["error"] = None
    state["iteration_count"] = state.get("iteration_count", 0) + 1
    return state