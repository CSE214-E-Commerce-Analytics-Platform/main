from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from graph import build_graph
import uvicorn

app = FastAPI(title="Text-to-SQL Agent API", version="1.0.0")

# Sadece Spring Boot backend'inden (localhost) erişime izin ver
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

# Graph'ı startup'ta bir kez oluştur
graph = build_graph()

class ChatRequest(BaseModel):
    question: str
    user_role: str       # JWT'den .NET tarafından gönderilir
    user_id: int         # JWT'den .NET tarafından gönderilir
    store_id: Optional[int] = None  # JWT'den .NET tarafından gönderilir

class ChatResponse(BaseModel):
    answer: str
    sql_query: Optional[str] = None
    visualization_code: Optional[str] = None

@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    """
    Sadece Spring Boot backend'i tarafından çağrılır.
    user_role, user_id, store_id frontend'den değil, JWT'den gelir.
    """
    state = {
        "question": request.question,
        "user_role": request.user_role,
        "user_id": request.user_id,
        "store_id": request.store_id,
        "sql_query": None,
        "query_result": None,
        "error": None,
        "final_answer": None,
        "visualization_code": None,
        "is_in_scope": None,
        "iteration_count": 0
    }

    try:
        result = graph.invoke(state)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")

    return ChatResponse(
        answer=result.get("final_answer") or "Cevap üretilemedi.",
        sql_query=result.get("sql_query"),
        visualization_code=result.get("visualization_code"),
    )

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    # Sadece localhost - dışarıya kapalı (AV-09 güvencesi)
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=False)
