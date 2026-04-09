from langchain_google_genai import ChatGoogleGenerativeAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

def analysis_agent(state: AgentState) -> AgentState:
    prompt = f"""Sen bir veri analistsin. Sorgu sonuçlarını sade Türkçe ile açıkla.
Sayıları yorumla, önemli bulguları vurgula. 2-3 cümle yeterli.

Soru: {state['question']}
Sorgu sonucu: {state['query_result']}"""

    response = llm.invoke([{"role": "user", "content": prompt}])
    state["final_answer"] = response.content
    return state