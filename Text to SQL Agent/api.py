import re
import sys
import io
import time
import threading
from collections import defaultdict, deque

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from graph import build_graph
import uvicorn

app = FastAPI(title="Text-to-SQL Agent API", version="2.0.0")

# ── CORS — only Spring Boot backend may call us ───────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

# ── Build the LangGraph pipeline once at startup ──────────────────────────────
graph = build_graph()

# ─────────────────────────────────────────────────────────────────────────────
# AV-09 FIX: In-memory rate limiter (per user_id)
#   • 20 requests / 60 seconds per user_id  →  blocks rapid enumeration loops
#   • 3 sequential ID-pattern queries in 30 s  →  anomaly block (10 min)
# ─────────────────────────────────────────────────────────────────────────────

_RATE_WINDOW   = 60          # seconds
_RATE_MAX      = 20          # max requests per window per user
_ENUM_WINDOW   = 30          # seconds to detect ID enumeration
_ENUM_THRESHOLD = 3          # consecutive ID-pattern queries = enumeration
_ENUM_BLOCK    = 600         # block duration after anomaly (seconds)

# {user_id: deque of timestamps}
_request_log: dict[int, deque] = defaultdict(deque)
# {user_id: unblock_timestamp}
_blocked_until: dict[int, float] = {}
# {user_id: deque of (timestamp, question) for enum detection}
_enum_log: dict[int, deque] = defaultdict(deque)

_lock = threading.Lock()

# Regex: questions that explicitly reference a numeric ID (store_id, order_id, user_id enumeration)
_ENUM_RE = re.compile(
    r'\b(?:store|order|user|product|sku)[^\d]{0,10}(\d+)\b'
    r'|\bid[^\d]{0,5}(\d+)\b',
    re.IGNORECASE
)

def _check_rate_limit(user_id: int, question: str) -> Optional[str]:
    """
    Returns None if the request is allowed, or an error string if blocked.
    Side-effect: records the request in the rate-limit and enum-detection logs.
    """
    now = time.monotonic()
    with _lock:
        # ── 1. Check if user is enumeration-blocked ───────────────────────
        unblock = _blocked_until.get(user_id, 0)
        if now < unblock:
            remaining = int(unblock - now)
            return f"Too many sequential enumeration attempts. Try again in {remaining}s."

        # ── 2. Sliding-window rate limit ──────────────────────────────────
        dq = _request_log[user_id]
        while dq and now - dq[0] > _RATE_WINDOW:
            dq.popleft()
        if len(dq) >= _RATE_MAX:
            return f"Rate limit exceeded. Maximum {_RATE_MAX} requests per {_RATE_WINDOW}s."
        dq.append(now)

        # ── 3. Enumeration anomaly detection ─────────────────────────────
        if _ENUM_RE.search(question):
            edq = _enum_log[user_id]
            while edq and now - edq[0][0] > _ENUM_WINDOW:
                edq.popleft()
            edq.append((now, question))
            if len(edq) >= _ENUM_THRESHOLD:
                _blocked_until[user_id] = now + _ENUM_BLOCK
                edq.clear()
                print(f"[RateLimit] ENUM BLOCK — user_id={user_id}, blocked for {_ENUM_BLOCK}s")
                return (
                    f"Sequential ID enumeration detected. "
                    f"Your account has been blocked for {_ENUM_BLOCK // 60} minutes."
                )

    return None  # allowed


# ─────────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str
    user_role: str          # injected from JWT by Spring Boot
    user_id: int            # injected from JWT by Spring Boot
    store_id: Optional[int] = None


class ChatResponse(BaseModel):
    answer: str
    sql_query: Optional[str] = None
    visualization_code: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    print(f"\n{'*'*50}")
    print(f"[API] REQUEST  user_id={request.user_id}  role={request.user_role}  store={request.store_id}")
    print(f"[API] QUESTION {request.question[:120]}")
    print(f"{'*'*50}\n")

    # ── AV-09: Rate limit + enumeration detection ─────────────────────────
    block_reason = _check_rate_limit(request.user_id, request.question)
    if block_reason:
        print(f"[RateLimit] BLOCKED user_id={request.user_id}: {block_reason}")
        raise HTTPException(status_code=429, detail=block_reason)

    state = {
        "question":          request.question,
        "user_role":         request.user_role,
        "user_id":           request.user_id,
        "store_id":          request.store_id,
        "sql_query":         None,
        "query_result":      None,
        "error":             None,
        "final_answer":      None,
        "visualization_code": None,
        "is_in_scope":       None,
        "iteration_count":   0,
        "intent":            None,
        "language":          None,
        "guardrail_reason":  None,
        "sql_error_type":    None,
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
    return {"status": "ok", "rate_limiter": "active"}


if __name__ == "__main__":
    # localhost only — external access blocked (AV-09)
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=False)
