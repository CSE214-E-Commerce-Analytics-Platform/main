from langchain_openai import ChatOpenAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

def analysis_agent(state: AgentState) -> AgentState:
    prompt = f"""Sen bir veri analistsin. Sorgu sonuçlarını sade Türkçe ile açıkla.
Sayıları yorumla, önemli bulguları vurgula. 2-3 cümle yeterli.

Soru: {state['question']}
Sorgu sonucu: {state['query_result']}"""

    response = llm.invoke([{"role": "user", "content": prompt}])
    state["final_answer"] = response.content
    return state