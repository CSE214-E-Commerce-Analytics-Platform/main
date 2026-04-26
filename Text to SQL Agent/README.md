# Text-to-SQL AI Agent

The AI agent is a Python microservice that translates natural language questions into PostgreSQL queries, executes them against the e-commerce database, and returns human-readable analysis with optional chart configurations. It is built with LangGraph for multi-agent orchestration and FastAPI for the HTTP interface.

---

## Tech Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11 | Runtime |
| FastAPI | latest | HTTP API server |
| Uvicorn | latest | ASGI server |
| LangGraph | latest | Multi-agent state machine orchestration |
| LangChain | latest | LLM integration utilities |
| OpenAI SDK | latest | LLM calls (GPT-4 via OpenAI API) |
| psycopg2-binary | latest | PostgreSQL driver |
| SQLAlchemy | latest | DB connection management |
| pandas | latest | Result set processing |
| python-dotenv | latest | Environment variable loading |

---

## Agent Pipeline

The agent processes each user question through a 5-node LangGraph state machine:

```
User Question
      │
      ▼
┌─────────────────┐
│ guardrails_agent │  ── Detects intent: sql_query | greeting | off_topic | unsafe
└────────┬────────┘
         │ (sql_query only)
         ▼
┌─────────────────┐
│   sql_agent     │  ── Generates PostgreSQL SELECT from natural language
└────────┬────────┘
         │
         ▼
┌─────────────────┐       ┌─────────────────┐
│  db_executor    │──────▶│  error_agent    │ (on SQL error → corrects & retries)
└────────┬────────┘       └─────────────────┘
         │
         ▼
┌─────────────────┐
│ analysis_agent  │  ── Produces human-readable insights from query results
└────────┬────────┘
         │
         ▼
┌──────────────────────┐
│ visualization_agent  │  ── Generates Chart.js configuration (bar, line, pie, etc.)
└──────────────────────┘
         │
         ▼
  JSON Response to Backend
```

### Node Descriptions

| Node | File | Responsibility |
|------|------|---------------|
| `guardrails_agent` | `agents/guardrails_agent.py` | Classifies intent, blocks prompt injection, off-topic requests, and unsafe operations. Supports English and Turkish. |
| `sql_agent` | `agents/sql_agent.py` | Uses the LLM to generate a safe `SELECT` query from the question and the database schema. Role-aware: CORPORATE users are automatically scoped to their own store. |
| `db_executor` | `tools/db_executor.py` | Executes the generated SQL. Blocks any query containing `DROP`, `DELETE`, `TRUNCATE`, or `UPDATE` at the string level before execution. |
| `error_agent` | `agents/error_agent.py` | Catches SQL exceptions (syntax errors, missing columns, permission issues) and feeds corrective instructions back to `sql_agent` for a retry. |
| `analysis_agent` | `agents/analysis_agent.py` | Reads the raw query result and produces a concise, business-oriented summary in the same language as the user's question. |
| `visualization_agent` | `agents/visualization_agent.py` | Decides whether a chart is appropriate and, if so, returns a Chart.js-compatible JSON configuration that the frontend renders directly. |

---

## API

### `POST /chat`

Accepts a natural language question and returns a structured analysis.

**Request Body:**
```json
{
  "question": "What are my top 5 best-selling products this month?",
  "user_role": "CORPORATE",
  "user_id": 42,
  "store_id": 7
}
```

**Response:**
```json
{
  "answer": "Your top 5 best-selling products this month are...",
  "visualization": {
    "type": "bar",
    "data": { ... },
    "options": { ... }
  },
  "sql_query": "SELECT p.name, SUM(oi.quantity) AS total_sold FROM ...",
  "is_in_scope": true
}
```

**Notes:**
- `user_role` controls data visibility. `CORPORATE` queries are filtered to the provided `store_id`.
- `INDIVIDUAL` and `ADMIN` roles see platform-wide data (subject to guardrail scope rules).
- If the question is off-topic or unsafe, `is_in_scope` is `false` and a polite redirect message is returned instead of SQL results.

---

## Security

- **Read-only enforcement:** `db_executor.py` rejects any query containing write/destructive keywords before it reaches the database.
- **Guardrails:** The `guardrails_agent` detects prompt injection attempts, jailbreak patterns, and questions unrelated to e-commerce analytics.
- **Rate limiting:** 20 requests per minute per `user_id`. Users who exceed the limit are blocked for 10 minutes. Enumeration attacks (probing for valid IDs) are detected and blocked.
- **CORS:** The FastAPI server only accepts requests from the Spring Boot backend (`localhost:8080`). It is not directly accessible from the browser.
- **Role-based scoping:** SQL generation is conditioned on the user's role and store ID to prevent data leakage between tenants.

---

## Running Locally

### Prerequisites

- Python 3.11+ — verify with `python --version`
- PostgreSQL 16 running at `localhost:5432` with the platform database populated
- An OpenAI API key

### Steps

```bash
# Navigate to the agent directory
cd "Text to SQL Agent"

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt

# Create the environment file
cp .env.example .env
# Edit .env and fill in OPENAI_API_KEY and DB credentials

# Start the server
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

The agent is available at **http://localhost:8000**.

The `--reload` flag enables hot-reload during development.

---

## Running with Docker

```bash
# From the project root
docker compose up --build ai-agent
```

The container is on the internal Docker network and only reachable by the backend service — it is not exposed on the host machine by default.

---

## Environment Variables

Create a `.env` file in the `Text to SQL Agent/` directory:

```env
# OpenAI
OPENAI_API_KEY=sk-proj-...

# Database
DB_HOST=localhost           # Use 'postgres' when running inside Docker
DB_PORT=5432
DB_NAME=ecommerce_analytics_platform_db
DB_USER=postgres
DB_PASSWORD=your_db_password
```

---

## Project Structure

```
Text to SQL Agent/
├── api.py              # FastAPI app, /chat endpoint, rate limiting
├── graph.py            # LangGraph StateGraph definition and node wiring
├── state.py            # AgentState TypedDict — shared state across all nodes
├── main.py             # Local test entry point (ask() helper)
├── requirements.txt    # Python dependencies
├── Dockerfile          # Container build
├── .env                # Secrets (not committed)
├── agents/
│   ├── guardrails_agent.py
│   ├── sql_agent.py
│   ├── error_agent.py
│   ├── analysis_agent.py
│   └── visualization_agent.py
└── tools/
    └── db_executor.py  # Safe SQL execution with read-only enforcement
```

---

## State Schema

`state.py` defines the shared `AgentState` TypedDict that flows through every node:

| Field | Type | Set By |
|-------|------|--------|
| `question` | `str` | API input |
| `user_role` | `str` | API input |
| `user_id` | `int` | API input |
| `store_id` | `int \| None` | API input |
| `intent` | `str` | `guardrails_agent` |
| `is_in_scope` | `bool` | `guardrails_agent` |
| `guardrail_reason` | `str` | `guardrails_agent` |
| `sql_query` | `str` | `sql_agent` |
| `query_result` | `list` | `db_executor` |
| `error` | `str \| None` | `db_executor` |
| `sql_error_type` | `str \| None` | `error_agent` |
| `final_answer` | `str` | `analysis_agent` |
| `visualization_code` | `dict \| None` | `visualization_agent` |
