from langchain_google_genai import ChatGoogleGenerativeAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

def visualization_agent(state: AgentState) -> AgentState:
    # Grafik gerekli mi?
    decision = llm.invoke([{
        "role": "user",
        "content": f"""Bu soru görsel grafik gerektiriyor mu?
Zaman serisi, karşılaştırma, dağılım, sıralama → 'yes'
Tekil sayı veya metin cevabı → 'no'
Sadece yes veya no yaz.

Soru: {state['question']}"""
    }])

    if "yes" not in decision.content.lower():
        state["visualization_code"] = None
        return state

    # Grafik kodu üret
    response = llm.invoke([{
        "role": "user",
        "content": f"""Plotly kullanarak Python kodu yaz.
Sadece kod yaz, markdown yok, açıklama yok. fig.show() ile bitir.

Veri: {state['query_result']}
Soru: {state['question']}"""
    }])

    state["visualization_code"] = response.content
    return state