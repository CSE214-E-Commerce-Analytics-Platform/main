from langchain_openai import ChatOpenAI
from state import AgentState
from dotenv import load_dotenv
load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

DECISION_PROMPT = """You are a data visualization assistant.
Decide whether the given question requires a chart or graph.

Answer 'yes' if the question involves: time series, comparisons, distributions, rankings, or trends.
Answer 'no' if the answer is a single number, a simple list, or plain text.

Respond with ONE word only: yes or no."""

CHART_PROMPT = """You are a Python data visualization expert.
Generate a Plotly chart using the provided data.

STRICT RULES:
- Output ONLY raw Python code. No markdown, no explanation, no code blocks.
- End the code with fig.show().
- Do NOT import data from external sources; use the data provided directly."""

def visualization_agent(state: AgentState) -> AgentState:
    # Decide if a chart is needed
    decision = llm.invoke([
        {"role": "system", "content": DECISION_PROMPT},
        {"role": "user", "content": f"Question: {state['question']}"}
    ])

    if "yes" not in decision.content.lower():
        state["visualization_code"] = None
        return state

    # Generate chart code
    response = llm.invoke([
        {"role": "system", "content": CHART_PROMPT},
        {"role": "user", "content": f"Data: {state['query_result']}\nQuestion: {state['question']}"}
    ])

    state["visualization_code"] = response.content
    return state