# CLAUDE.md - Full-Stack E-Commerce & AI Project Context

## 🏗️ Project Overview
This is an enterprise-grade e-commerce platform consisting of three main modules:
1. **Backend:** Java 21+ & Spring Boot 3.x (REST API, Core Business Logic, PostgreSQL/MySQL).
2. **Frontend:** Angular (Standalone components, Feature-based architecture).
3. **AI Module:** Python & LangGraph (Text-to-SQL Agent for natural language database querying and analytics).

---

## ⚙️ 1. BACKEND RULES (Spring Boot)
**Directory:** `/furkan` (or project root depending on workspace)
- **Architecture:** Controller -> Service (Interface + Impl) -> Repository.
- **Global Pagination (CRITICAL):** - ALL list-returning endpoints MUST accept `RestPageableRequest` as a parameter (`@ModelAttribute` in GET).
  - ALL list-returning endpoints MUST return `RootEntity<RestPageableEntity<Dto...>>`. 
  - NEVER return direct `List<T>` for data collections (except for specific analytics aggregations).
- **Standard Response:** Every API response is wrapped in `RootEntity<T>`.
- **Business Rules:**
  - **Corporate & Store (1:1 Strict):** A user with `CORPORATE` role has exactly ONE `Store`. Multi-store logic is completely removed. Auto-select the first store from `response.content[0]`.
  - **Orders:** Uses Master-SubOrder architecture. A cart with items from multiple stores is split into one Parent Order and multiple Child Orders.
- **Exception Handling:** Use custom `BaseException` containing an `ErrorMessage` mapped to `MessageType` enums.
- **DTO Mapping:** Use custom manual mappers (e.g., `this::dtoConverter`) inside Service Impls, utilizing `BeanUtils.copyProperties`.

---

## 🎨 2. FRONTEND RULES (Angular)
**Directory:** `/src`
- **Tech Stack:** Angular 17+ (Standalone Components, `app.routes.ts`, `app.config.ts`).
- **Hybrid Pagination Strategy:**
  - *Admin/Corporate Views* (Heavy Data): Implement UI pagination controls (`← Previous`, `Next →`). Extract data from `response.payload.content`.
  - *Individual Views* (Low Volume): Send default payload `{ pageNumber: 0, pageSize: 100 }` to avoid building unnecessary pagination UI.
- **Folder Structure:**
  - `/app/core`: Services, Interceptors (`auth.interceptor.ts`), Guards (`role.guard.ts`).
  - `/app/shared`: Models (`pageable.ts`, `api-response.ts`), common UI widgets.
  - `/app/features`: Domain-specific modules (`admin`, `corporate`, `individual`, `auth`, `chatbot`).
- **Corporate UI Restrictions:** NO "Create Store" UI elements. The Corporate panel must auto-bind to the user's single existing store ID without dropdowns.

---

## 🧠 3. AI AGENT RULES (Python / LangGraph)
**Directory:** `/Text to SQL Agent`
- **Tech Stack:** Python, LangGraph, FastAPI (`api.py`), SQLAlchemy/DB utilities.
- **Workflow / Graph Architecture (`graph.py`, `state.py`):**
  1. `guardrails_agent`: Ensures the user query is safe and relevant to the business context.
  2. `sql_agent`: Translates natural language into PostgreSQL/MySQL queries.
  3. `db_executor`: Executes the SQL safely.
  4. `error_agent`: Catches SQL errors and feeds corrections back to the `sql_agent`.
  5. `analysis_agent` & `visualization_agent`: Processes DB results into human-readable insights and chart configurations.
- **Safety Rule:** The AI must NEVER execute destructive queries (`DROP`, `DELETE`, `TRUNCATE`). Only `SELECT` operations are permitted in the Text-to-SQL flow.

---

## 🛠️ Cross-Module Collaboration
- When writing Frontend code that calls the Backend, always check the corresponding `DtoResponse` and ensure `RestPageableEntity` unpacking is handled.
- When adjusting the Database schema in the Backend, remember to inform the AI Agent (e.g., updating DB schema prompts for `sql_agent.py`) so the AI knows the new column names.
- Provide step-by-step reasoning before making architectural changes across these three domains.