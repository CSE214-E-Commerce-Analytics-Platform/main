from langchain_openai import ChatOpenAI
from state import AgentState
from dotenv import load_dotenv
import urllib.parse
import json

load_dotenv(override=True)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

DECISION_PROMPT = """You are a data visualization assistant.
Decide whether the given question requires a chart or graph.
Answer 'yes' if the question involves: time series, comparisons, distributions, rankings, or trends.
Answer 'no' if the answer is a single number, a simple list, or plain text.
Respond with ONE word only: yes or no."""

CHART_PROMPT = """You are a data visualization expert.
Generate a strictly valid Chart.js JSON configuration object to visualize the data.
Use simple bar, pie, or line charts. Make it colorful and modern.

STRICT RULE:
- Always set 'beginAtZero' to true for the Y-axis (ticks) to ensure all data points are visible.
- DO NOT wrap the output in markdown blocks like ```json. Output ONLY the raw JSON object starting with { and ending with }."""

def visualization_agent(state: AgentState) -> AgentState:
    # 1. Grafiğe gerek var mı diye sor
    decision = llm.invoke([
        {"role": "system", "content": DECISION_PROMPT},
        {"role": "user", "content": f"Question: {state['question']}"}
    ])

    if "yes" not in decision.content.lower():
        return state

    # 2. Gerek varsa grafiğin JSON verisini oluştur
    response = llm.invoke([
        {"role": "system", "content": CHART_PROMPT},
        {"role": "user", "content": f"Data: {state['query_result']}\nQuestion: {state['question']}"}
    ])

    chart_json = response.content.strip()
    
    # LLM inatla markdown (```json) eklerse diye temizlik yapalım
    if chart_json.startswith("```"):
        chart_json = chart_json.split("\n", 1)[-1]
        if chart_json.endswith("```"):
            chart_json = chart_json.rsplit("\n", 1)[0]
    chart_json = chart_json.replace("```json", "").replace("```", "").strip()

    try:
        # Geçerli bir JSON mu test et
        json.loads(chart_json)
        
        # JSON'ı QuickChart API üzerinden bir resim (Image) URL'sine çevir
        encoded_chart = urllib.parse.quote(chart_json)
        image_url = f"https://quickchart.io/chart?c={encoded_chart}&w=500&h=300"
        
        # Üretilen resim linkini Markdown formatında metnin sonuna ekle!
        if state.get("final_answer"):
            state["final_answer"] += f"\n\n![Grafik]({image_url})"
        else:
            state["final_answer"] = f"Here is your graph:\n\n![Grafik]({image_url})"
            
    except Exception as e:
        print("Graph could not be created:", e)

    return state