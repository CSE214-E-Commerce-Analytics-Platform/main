# E-Commerce Analytics Platform

A full-stack, enterprise-grade e-commerce platform with AI-powered natural language analytics. The system is built across three independent modules that communicate over HTTP and share a single PostgreSQL database.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Client Browser                      │
│                   Angular 19 SPA (:4200)                 │
└──────────────────────────┬──────────────────────────────┘
                           │ REST (HTTP)
┌──────────────────────────▼──────────────────────────────┐
│               Spring Boot Backend (:8080)                 │
│   Auth · Products · Orders · Payments · Analytics · AI   │
└───────────┬──────────────────────────┬───────────────────┘
            │ SQL (JDBC)               │ HTTP
┌───────────▼──────────┐  ┌───────────▼───────────────────┐
│  PostgreSQL (:5432)  │  │  Text-to-SQL AI Agent (:8000) │
│  17-table schema     │  │  LangGraph + FastAPI           │
└──────────────────────┘  └───────────────────────────────┘
```

### Modules

| Module | Technology | Port |
|--------|-----------|------|
| **Backend** | Java 21, Spring Boot 3.5, Maven | `8080` |
| **Frontend** | Angular 19, TypeScript 5.7 | `4200` (dev) / `80` (prod) |
| **AI Agent** | Python 3.11, LangGraph, FastAPI | `8000` |
| **Database** | PostgreSQL 16 | `5432` |

---

## User Roles

The platform supports three distinct user roles, each with its own dashboard and permission set:

| Role | Description |
|------|-------------|
| **INDIVIDUAL** | Browse products, place orders, write reviews, access personal analytics |
| **CORPORATE** | Manage a single store, track inventory, view business analytics, process orders |
| **ADMIN** | Full platform control — users, stores, categories, audit logs, system settings |

---

## Key Features

- **Authentication:** Email/password with email verification, Google OAuth2, GitHub OAuth2, JWT access + refresh token rotation
- **Shopping:** Browse by store or category, cart management, multi-store order splitting, Stripe payment integration
- **Order Management:** Master-SubOrder architecture for carts containing items from multiple stores
- **Shipment Tracking:** Real-time status tracking from warehouse to delivery
- **Reviews & Sentiment:** Star ratings with automatic sentiment classification (POSITIVE / NEUTRAL / NEGATIVE)
- **Analytics Dashboards:** Charts and KPIs for both individual buyers and corporate sellers
- **AI Chatbot:** Natural language queries over the database — ask business questions in plain English or Turkish, get SQL results with visualizations
- **Audit Logging:** Full action trail for admin oversight

---

## Quick Start — Docker (Recommended)

All services are orchestrated with Docker Compose. This is the fastest way to run the entire platform.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A valid `.env` file in the project root (see **Environment Variables** below)

### Run Everything

```bash
# Clone the repository
git clone https://github.com/CSE214-E-Commerce-Analytics-Platform/main.git
cd main

# Copy and fill in the environment file
cp .env.example .env
# Edit .env with your credentials

# Start all services
docker compose up --build
```

Once running, access the app at **http://localhost** (port 80).

---

## Quick Start — Local Development

If you prefer to run each service individually (e.g., for hot-reload development), follow the per-module guides:

| Module | Guide |
|--------|-------|
| Backend | [`backend/README.md`](./backend/README.md) |
| Frontend | [`frontend/e-commerce-frontend/README.md`](./frontend/e-commerce-frontend/README.md) |
| AI Agent | [`Text to SQL Agent/README.md`](<./Text to SQL Agent/README.md>) |
| Database | [`database/README.md`](./database/README.md) |

---

## Environment Variables

Create a `.env` file in the project root. All services read from this file via Docker Compose.

```env
# ── Database ──────────────────────────────────────────────
DB_USERNAME=postgres
DB_PASS=your_db_password

# ── JWT ───────────────────────────────────────────────────
JWT_SECRET=your_jwt_secret_key_at_least_32_chars

# ── AI Agent ──────────────────────────────────────────────
OPENAI_API_KEY=sk-proj-...

# ── Email (Mailtrap sandbox) ──────────────────────────────
MAIL_HOST=sandbox.smtp.mailtrap.io
MAIL_PORT=2525
MAIL_USERNAME=your_mailtrap_username
MAIL_PASSWORD=your_mailtrap_password

# ── Google OAuth2 ─────────────────────────────────────────
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret

# ── GitHub OAuth2 ─────────────────────────────────────────
GITHUB_CLIENT_ID=your_github_client_id
GITHUB_CLIENT_SECRET=your_github_client_secret

# ── Stripe ────────────────────────────────────────────────
STRIPE_API_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...
```

---

## Database Schema

> **Visual ER Diagram:** [`database/ecommerce_platform_db_diagram.pdf`](./database/ecommerce_platform_db_diagram.pdf)

The platform uses **18 tables** managed by Hibernate (`ddl-auto=update`). See [`database/README.md`](./database/README.md) for the full schema reference.

---

## Project Structure

```
main/
├── backend/                    # Spring Boot REST API
│   └── ecommerce-backend/
├── frontend/                   # Angular SPA
│   └── e-commerce-frontend/
├── Text to SQL Agent/          # Python AI agent
├── database/                   # Seed & ETL scripts
│   └── new_etl/
├── docker-compose.yml          # Multi-service orchestration
├── .env                        # Environment secrets (not committed)
└── .github/workflows/          # CI/CD pipelines
```

---

## Tech Stack Summary

| Layer | Technologies |
|-------|-------------|
| **Backend** | Java 21, Spring Boot 3.5, Spring Security, Spring Data JPA, Hibernate, JJWT, Stripe SDK, SpringDoc OpenAPI |
| **Frontend** | Angular 19, TypeScript 5.7, Bootstrap 5, Chart.js 4, RxJS 7, html2canvas, jsPDF, xlsx |
| **AI Agent** | Python 3.11, LangGraph, FastAPI, LangChain, OpenAI SDK, SQLAlchemy, psycopg2 |
| **Database** | PostgreSQL 16 |
| **DevOps** | Docker, Docker Compose, Nginx, GitHub Actions |
