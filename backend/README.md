# Backend — Spring Boot REST API

The backend is a RESTful API built with Java 21 and Spring Boot 3.5. It serves as the central hub of the platform, handling authentication, business logic, payment processing, and communication with the AI agent.

---

## Tech Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| Java | 21 (LTS) | Runtime |
| Spring Boot | 3.5.x | Application framework |
| Spring Security | 6.x | Authentication & authorization |
| Spring Data JPA / Hibernate | 6.x | ORM, `ddl-auto=update` |
| PostgreSQL Driver | 42.x | Database connectivity |
| JJWT | 0.12.6 | JWT access & refresh tokens |
| Spring OAuth2 Client | — | Google & GitHub social login |
| Stripe Java SDK | 24.22.0 | Payment processing |
| Spring Mail | — | Email verification (Mailtrap sandbox) |
| SpringDoc OpenAPI | 2.8.5 | Swagger UI / API docs |
| Lombok | — | Boilerplate reduction |
| Maven | 3.9 | Build tool |

---

## Architecture

The backend follows a strict layered architecture:

```
Controller  →  Service Interface  →  Service Implementation  →  Repository
                                                                     ↕
                                                              PostgreSQL (JPA)
```

- **Controllers** (`IRestXxxController` + `RestXxxController`): Receive HTTP requests, validate input, delegate to services.
- **Services** (`IXxxService` + `XxxServiceImpl`): Contain all business logic and DTO mapping.
- **Repositories** (`XxxRepository extends JpaRepository`): Data access via Spring Data.
- **Entities**: JPA-managed domain objects mapped to database tables.
- **DTOs**: Separate request/response objects; mapped manually with `BeanUtils.copyProperties`.

### Standard Response Wrapper

Every API response is wrapped in `RootEntity<T>`:

```json
{
  "status": 200,
  "message": "Success",
  "payload": { ... }
}
```

### Pagination

All list endpoints use `RestPageableRequest` as input and return `RootEntity<RestPageableEntity<T>>`:

```json
{
  "payload": {
    "content": [...],
    "totalElements": 150,
    "totalPages": 3,
    "pageNumber": 0,
    "pageSize": 50
  }
}
```

### Exception Handling

Custom `BaseException` carries an `ErrorMessage` mapped to `MessageType` enums, returned as structured JSON error responses.

---

## API Endpoints

The full interactive API documentation is available at **http://localhost:8080/swagger-ui.html** when the server is running.

| Domain | Base Path | Key Operations |
|--------|-----------|---------------|
| Auth | `/api/auth` | Register, login, refresh token, verify email, forgot/reset password, OAuth2 callback |
| Users | `/api/users` | Profile management, role management (Admin) |
| Stores | `/api/stores` | Store CRUD (Corporate 1:1 binding) |
| Products | `/api/products` | Product CRUD with ownership checks, search, filter |
| Categories | `/api/categories` | Hierarchical category tree |
| Cart | `/api/cart` | Add/remove items, view cart |
| Orders | `/api/orders` | Place order (multi-store split), order history |
| Payments | `/api/payments` | Stripe checkout, webhook handling |
| Shipments | `/api/shipments` | Tracking, status updates |
| Reviews | `/api/reviews` | Create/read reviews with sentiment |
| Addresses | `/api/addresses` | User address book |
| Analytics | `/api/analytics` | Aggregated KPIs and chart data |
| AI | `/api/ai` | Proxy to Text-to-SQL agent |
| Chat History | `/api/chat-histories` | Persist AI conversation sessions |
| Audit Logs | `/api/audit-logs` | Admin audit trail |
| Customer Profiles | `/api/customer-profiles` | Demographic data |
| Corporate Requests | `/api/corporate-update-requests` | Role upgrade workflow |

---

## Security

- **JWT:** Short-lived access tokens (15 min) + long-lived refresh tokens (7 days) with rotation. Stored in HttpOnly cookies / `Authorization` header.
- **OAuth2:** Google and GitHub social login with automatic account linking.
- **Role-Based Access Control:** `INDIVIDUAL`, `CORPORATE`, `ADMIN` roles enforced at the controller level via Spring Security.
- **Stripe Webhooks:** Verified with `STRIPE_WEBHOOK_SECRET` to prevent spoofed payment events.
- **Email Verification:** New accounts must verify their email before accessing the platform.
- **Password Reset:** Time-limited tokens sent via email.

---

## Running Locally

### Prerequisites

- Java 21 SDK installed (`java -version` should print `21`)
- Maven 3.9+ installed
- PostgreSQL 16 running on `localhost:5432`
- A populated `.env` file (see root `README.md`)

### Steps

```bash
# Navigate to the backend directory
cd backend/ecommerce-backend

# Copy application properties template (if .env not auto-loaded)
# Ensure src/main/resources/application.properties points to your local DB

# Build and run
./mvnw spring-boot:run
```

The server starts at **http://localhost:8080**.

### Running with Docker

```bash
# From the project root
docker compose up --build backend
```

This also starts the `postgres` and `ai-agent` containers that the backend depends on.

---

## Configuration

Key settings in `src/main/resources/application.properties`:

```properties
# Server
server.port=8080

# Database
spring.datasource.url=jdbc:postgresql://localhost:5432/ecommerce_analytics_platform_db

# JPA
spring.jpa.hibernate.ddl-auto=update

# JWT
jwt.expiration=900000          # 15 minutes (ms)
jwt.refresh-expiration=604800000  # 7 days (ms)

# CORS
cors.allowed-origins=http://localhost:4200

# AI Agent
ai.agent.url=http://127.0.0.1:8000/chat

# Swagger
springdoc.swagger-ui.path=/swagger-ui.html
```

---

## Project Structure

```
ecommerce-backend/
├── src/main/java/com/furkan/
│   ├── config/             # Security, CORS, Stripe, OpenAPI config beans
│   ├── controllers/        # REST controllers (interface + implementation)
│   ├── dto/                # Request & response DTOs
│   ├── entities/           # JPA entity classes
│   ├── enums/              # MessageType, RoleType, OrderStatus, etc.
│   ├── exception/          # BaseException, ErrorMessage
│   ├── handler/            # GlobalExceptionHandler
│   ├── repositories/       # Spring Data JPA repositories
│   ├── scheduler/          # Scheduled tasks
│   ├── security/           # JWT filter, OAuth2 handlers, UserDetailsService
│   ├── services/           # Service interfaces + implementations
│   ├── starter/            # Application startup & initialization
│   └── utils/              # Shared utilities
└── src/main/resources/
    └── application.properties
```

---

## Business Rules

- **Corporate & Store (1:1):** A `CORPORATE` user owns exactly one `Store`. There is no multi-store support.
- **Order Splitting:** When a cart contains items from multiple stores, one parent `Order` and one child `Order` per store are created automatically.
- **Payment Flow:** A payment record is created with status `PENDING`, then updated to `SUCCESS` or `FAILED` via the Stripe webhook.
- **Audit Logging:** Key user actions (login, order placement, admin changes) are automatically written to the `audit_logs` table.
