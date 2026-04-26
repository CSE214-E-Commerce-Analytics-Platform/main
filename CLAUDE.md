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

## 🗄️ 4. DATABASE
- **Engine:** PostgreSQL, running **locally on port 5432**.
- **DB adı:** `ecommerce_analytics_platform_db`
- **Local connection:** `localhost:5432`
- `application.properties` ve AI agent `.env` dosyasındaki `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` değerleri local PostgreSQL'i işaret eder.
- Docker ortamında `postgres` adlı bir servis olarak container içinde çalışır (bkz. `docker-compose.yml`).
- **Schema Management:** Hibernate `ddl-auto=update` — tablolar entity sınıflarından otomatik oluşturulur, manuel SQL migration dosyası yoktur.

### 4.1 DATABASE SCHEMA

**Enum Storage Convention (CRITICAL for seed/ETL scripts):**
- `@Enumerated(EnumType.STRING)` → DB'de metin olarak saklanır: `provider`, `role_type` (users), `status` (orders), `payment_method` + `status` (payments), `sentiment` (reviews), `status` (corporate_update_requests), `token_type` (verification_tokens), `user_role` (audit_logs)
- `@Enumerated` YOK → JPA default **ORDINAL** (integer) olarak saklanır: `status` (shipments), `membership_type` (customer_profiles)

---

#### `users`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| email | VARCHAR UNIQUE NOT NULL | |
| password_hash | VARCHAR | BCrypt hash |
| provider | VARCHAR | EnumType.STRING: `LOCAL`, `GOOGLE` |
| role_type | VARCHAR | EnumType.STRING: `INDIVIDUAL`, `CORPORATE`, `ADMIN` |
| is_active | BOOLEAN | default true |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `stores`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| name | VARCHAR NOT NULL | |
| status | VARCHAR | e.g. `ACTIVE` |
| owner_id | BIGINT UNIQUE NOT NULL | FK → users(id), 1:1 |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `categories`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| name | VARCHAR NOT NULL | |
| parent_id | BIGINT NULL | FK → categories(id), self-referential |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `products`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| name | VARCHAR NOT NULL | |
| description | TEXT | |
| image_url | VARCHAR | Format: `cat-{slug}.jfif` or `.webp` |
| sku | VARCHAR UNIQUE NOT NULL | |
| unit_price | DECIMAL | |
| stock_quantity | INTEGER | |
| store_id | BIGINT NOT NULL | FK → stores(id) |
| category_id | BIGINT NULL | FK → categories(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `carts`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| total_price | DECIMAL NOT NULL | default 0.00 |
| user_id | BIGINT UNIQUE NOT NULL | FK → users(id), 1:1 |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `cart_items`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| quantity | INT NOT NULL | |
| cart_id | BIGINT NOT NULL | FK → carts(id) |
| product_id | BIGINT NOT NULL | FK → products(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `orders`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| status | VARCHAR | EnumType.STRING: `PENDING`, `PAID`, `SHIPPED`, `PARTIALLY_SHIPPED`, `DELIVERED`, `CANCELLED` |
| grand_total | DECIMAL(10,2) | |
| order_date | TIMESTAMP | |
| shipping_cost | DECIMAL(10,2) | |
| user_id | BIGINT NOT NULL | FK → users(id) |
| store_id | BIGINT NULL | FK → stores(id); NULL for parent orders |
| parent_order_id | BIGINT NULL | FK → orders(id); NULL for top-level orders |
| address_id | BIGINT NOT NULL | FK → addresses(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `order_items`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| quantity | INT | |
| price | DECIMAL | |
| product_id | BIGINT NOT NULL | FK → products(id) |
| order_id | BIGINT NOT NULL | FK → orders(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `payments`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| order_id | BIGINT UNIQUE NOT NULL | FK → orders(id), 1:1 |
| amount | DECIMAL(18,2) NOT NULL | |
| payment_method | VARCHAR NOT NULL | EnumType.STRING: `CREDIT_CARD`, `BANK_TRANSFER`, `CASH_ON_DELIVERY` |
| status | VARCHAR NOT NULL | EnumType.STRING: `SUCCESS`, `PENDING`, `FAILED` |
| transaction_key | VARCHAR NOT NULL | UUID |
| error_message | TEXT NULL | |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `shipments`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| warehouse | VARCHAR | e.g. `Istanbul-Main` |
| tracking_number | VARCHAR | |
| mode | VARCHAR | `Standard`, `Express`, `Same Day` |
| status | SMALLINT | **ORDINAL**: 0=PENDING, 1=LABEL_CREATED, 2=IN_TRANSIT, 3=OUT_FOR_DELIVERY, 4=DELIVERED, 5=RETURNED, 6=CANCELLED |
| estimated_delivery_date | TIMESTAMP | |
| order_id | BIGINT NOT NULL | FK → orders(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `reviews`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| star_rating | INTEGER NOT NULL | 1-5 |
| sentiment | VARCHAR(50) | EnumType.STRING: `POSITIVE`, `NEUTRAL`, `NEGATIVE` |
| comment_text | TEXT | |
| product_id | BIGINT NOT NULL | FK → products(id) |
| user_id | BIGINT NOT NULL | FK → users(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `addresses`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| user_id | BIGINT NOT NULL | FK → users(id) |
| city | VARCHAR NOT NULL | |
| district | VARCHAR NOT NULL | |
| full_address | VARCHAR NOT NULL | |
| phone_number | VARCHAR NOT NULL | |
| zip_code | VARCHAR NOT NULL | |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `customer_profiles`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| user_id | BIGINT UNIQUE NOT NULL | FK → users(id), 1:1 |
| age | INT | |
| city | VARCHAR(100) | |
| state | VARCHAR(100) | |
| country | VARCHAR(50) | |
| membership_type | SMALLINT | **ORDINAL**: 0=STANDARD, 1=PREMIUM, 2=VIP |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `corporate_update_requests`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| company_name | VARCHAR NOT NULL | |
| reason | TEXT NOT NULL | |
| status | VARCHAR | EnumType.STRING: `PENDING`, `APPROVED`, `REJECTED` |
| admin_note | TEXT NULL | |
| user_id | BIGINT NOT NULL | FK → users(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `audit_logs`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| user_id | BIGINT NOT NULL | Denormalized (no FK constraint) |
| user_role | VARCHAR NOT NULL | EnumType.STRING: `INDIVIDUAL`, `CORPORATE`, `ADMIN` |
| action | VARCHAR NOT NULL | |
| details | TEXT | |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `refresh_tokens`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| token | VARCHAR UNIQUE NOT NULL | |
| expires_at | TIMESTAMP NOT NULL | |
| revoked_at | TIMESTAMP NULL | |
| replaced_by | BIGINT NULL | FK → refresh_tokens(id), self-referential |
| user_id | BIGINT | FK → users(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `verification_tokens`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| token | VARCHAR UNIQUE NOT NULL | |
| token_type | VARCHAR NOT NULL | EnumType.STRING: `EMAIL_VERIFICATION`, `PASSWORD_RESET` |
| expires_at | TIMESTAMP NOT NULL | |
| used_at | TIMESTAMP NULL | |
| user_id | BIGINT NOT NULL | FK → users(id) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

#### `chat_histories`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | IDENTITY |
| title | VARCHAR NOT NULL | First 50 chars of the initial query |
| initial_query | TEXT NOT NULL | The full first question asked |
| user_id | BIGINT NOT NULL | Denormalized (no FK constraint) |
| created_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

**API:** `GET/POST /api/chat-histories`, `PATCH /api/chat-histories/{id}/title`, `DELETE /api/chat-histories/{id}`
**Frontend service:** `ChatHistoryService` — `getAll()` fetches paginated (pageSize:100), maps `.content`; `id` type is `number`.

---

### 4.2 ETL & Seed Scripts
- **Location:** `/database/new_etl/`
- **seed.py:** Faker ile additive sahte veri ekler. `ON CONFLICT DO NOTHING` ile güvenli upsert.
- **etl_csv.py:** Gerçek CSV datasını yükler (Amazon ürünleri + müşteri davranışları). `seed.py`'den SONRA çalıştırılmalı.
- **CSV kaynak dizini:** `C:\Users\Furkan\Desktop\CSE214_Project_Gereksinimler\datasets\`
- **Eski ETL** (`/database/etl/`): MySQL tabanlı, artık kullanılmıyor.

---

## 🛠️ 5. Cross-Module Collaboration
- When writing Frontend code that calls the Backend, always check the corresponding `DtoResponse` and ensure `RestPageableEntity` unpacking is handled.
- When adjusting the Database schema in the Backend, remember to inform the AI Agent (e.g., updating DB schema prompts for `sql_agent.py`) so the AI knows the new column names.
- Provide step-by-step reasoning before making architectural changes across these three domains.