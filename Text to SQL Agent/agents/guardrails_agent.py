from langchain_google_genai import ChatGoogleGenerativeAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

SYSTEM_PROMPT = """Sen bir e-ticaret platformunun güvenlik filtresisin.
Kullanıcının sorusunu analiz et ve şu kategorilerden birine koy:

- greeting: Selamlama veya genel sohbet
- in_scope: E-ticaret analitiği ile ilgili (satış, sipariş, ürün, müşteri, gelir, kargo)
- out_of_scope: E-ticaret ile alakasız sorular

Sadece şu üç kelimeden birini yaz: greeting, in_scope, out_of_scope"""

def guardrails_agent(state: AgentState) -> AgentState:
    response = llm.invoke([
        {"role": "user", "content": f"{SYSTEM_PROMPT}\n\nSoru: {state['question']}"}
    ])
    
    result = response.content.strip().lower()
    
    if "greeting" in result:
        state["is_in_scope"] = False
        state["final_answer"] = "Merhaba! E-ticaret verilerinizi analiz etmek için buradayım."
    elif "out_of_scope" in result:
        state["is_in_scope"] = False
        state["final_answer"] = "Üzgünüm, yalnızca e-ticaret analitiği konularında yardımcı olabilirim."
    else:
        state["is_in_scope"] = True
    
    return state