# Database

The platform uses **PostgreSQL 16** as its primary data store. Schema management is handled automatically by Hibernate (`ddl-auto=update`) — tables are created and updated from the Java entity classes; there are no manual SQL migration files.

---

## Connection Details

| Setting | Local Dev | Docker |
|---------|-----------|--------|
| Host | `localhost` | `postgres` (Docker service name) |
| Port | `5432` | `5432` |
| Database | `ecommerce_analytics_platform_db` | same |
| User | `postgres` | set via `DB_USERNAME` env var |
| Password | `your_password` | set via `DB_PASS` env var |

---

## ER Diagram

The full ER diagram is available as a PDF in this directory: [`ecommerce_platform_db_diagram.pdf`](./ecommerce_platform_db_diagram.pdf).

---

## Schema Overview

The database contains **18 tables** organised around the core e-commerce domains:

| Domain | Tables |
|--------|--------|
| **Identity** | `users`, `refresh_tokens`, `verification_tokens` |
| **Store & Products** | `stores`, `categories`, `products` |
| **Shopping** | `carts`, `cart_items` |
| **Orders** | `orders`, `order_items`, `payments`, `shipments` |
| **Engagement** | `reviews`, `addresses` |
| **Profiles** | `customer_profiles`, `corporate_update_requests` |
| **Platform** | `audit_logs`, `chat_histories` |

---

## Enum Storage Convention

Some columns store enum values differently — this is critical for seed scripts and raw SQL queries.

### Stored as STRING (`EnumType.STRING`)

These columns contain the enum name as plain text:

| Table | Column | Values |
|-------|--------|--------|
| `users` | `provider` | `LOCAL`, `GOOGLE` |
| `users` | `role_type` | `INDIVIDUAL`, `CORPORATE`, `ADMIN` |
| `orders` | `status` | `PENDING`, `PAID`, `SHIPPED`, `PARTIALLY_SHIPPED`, `DELIVERED`, `CANCELLED` |
| `payments` | `payment_method` | `CREDIT_CARD`, `BANK_TRANSFER`, `CASH_ON_DELIVERY` |
| `payments` | `status` | `SUCCESS`, `PENDING`, `FAILED` |
| `reviews` | `sentiment` | `POSITIVE`, `NEUTRAL`, `NEGATIVE` |
| `corporate_update_requests` | `status` | `PENDING`, `APPROVED`, `REJECTED` |
| `verification_tokens` | `token_type` | `EMAIL_VERIFICATION`, `PASSWORD_RESET` |
| `audit_logs` | `user_role` | `INDIVIDUAL`, `CORPORATE`, `ADMIN` |

### Stored as ORDINAL (integer — JPA default)

These columns store the zero-based position of the enum constant:

| Table | Column | Mapping |
|-------|--------|---------|
| `shipments` | `status` | `0=PENDING`, `1=LABEL_CREATED`, `2=IN_TRANSIT`, `3=OUT_FOR_DELIVERY`, `4=DELIVERED`, `5=RETURNED`, `6=CANCELLED` |
| `customer_profiles` | `membership_type` | `0=STANDARD`, `1=PREMIUM`, `2=VIP` |

---

## Table Reference

| Table | References | Notes |
|-------|-----------|-------|
| `users` | — | Root entity; referenced by almost every other table |
| `stores` | `users` (owner_id) | Strict 1:1 — each corporate user owns exactly one store |
| `categories` | `categories` (parent_id) | Self-referential; `parent_id` nullable for top-level categories |
| `products` | `stores` (store_id), `categories` (category_id) | `category_id` nullable |
| `carts` | `users` (user_id) | 1:1 with user |
| `cart_items` | `carts` (cart_id), `products` (product_id) | Junction table |
| `addresses` | `users` (user_id) | A user can have multiple addresses |
| `orders` | `users` (user_id), `addresses` (address_id), `stores` (store_id), `orders` (parent_order_id) | `store_id` and `parent_order_id` nullable — NULL on master/parent orders |
| `order_items` | `orders` (order_id), `products` (product_id) | Junction table |
| `payments` | `orders` (order_id) | 1:1 with order |
| `shipments` | `orders` (order_id) | One shipment per child order |
| `reviews` | `products` (product_id), `users` (user_id) | |
| `customer_profiles` | `users` (user_id) | 1:1 with user |
| `corporate_update_requests` | `users` (user_id) | Role upgrade workflow |
| `refresh_tokens` | `users` (user_id), `refresh_tokens` (replaced_by) | Self-referential for token rotation chain |
| `verification_tokens` | `users` (user_id) | Email verification & password reset |
| `audit_logs` | — | `user_id` stored as plain integer, no FK constraint |
| `chat_histories` | — | `user_id` stored as plain integer, no FK constraint |

---

## Seed & ETL Scripts

All scripts are in `database/new_etl/`. Run them **after** the backend has started at least once so Hibernate creates the tables.

### Order of Execution

```
1. (Start backend once — Hibernate creates all tables)
2. seed.py
3. etl_csv.py
4. seed_audit_logs.py   (optional)
5. seed_reviews.py      (optional)
6. seed_extra.py        (optional)
```

### `seed.py` — Faker-based Synthetic Data

Generates realistic fake data using the Faker library. Safe to run multiple times — uses `ON CONFLICT DO NOTHING`.

```bash
cd database/new_etl
pip install -r requirements.txt    # if not already installed
python seed.py
```

Creates: users (all three roles), stores, categories, products, carts, orders, payments, shipments, reviews, addresses, customer profiles.

### `etl_csv.py` — Real CSV Data Import

Loads real-world product and customer behaviour data from CSV files. Must be run **after** `seed.py`.

```bash
python etl_csv.py
```

**Required CSV files** (place them in the same directory as `etl_csv.py` or update the path at the top of the script):

| File | Loads Into |
|------|-----------|
| `amazon_sales.csv` | `products`, `reviews` |
| `E-commerce_Customer_Behavior.csv` | `users` (INDIVIDUAL), `customer_profiles` |

### Other Scripts

| Script | Purpose |
|--------|---------|
| `seed_audit_logs.py` | Populates the `audit_logs` table with historical action records |
| `seed_reviews.py` | Adds additional review data with varied sentiment distribution |
| `seed_extra.py` | Additional seeding for edge-case testing scenarios |

---

## Running PostgreSQL Locally

### Option 1 — Docker (Recommended)

```bash
# From the project root
docker compose up postgres
```

This starts PostgreSQL 16-alpine with a persistent volume (`postgres_data`). The database is ready for connections at `localhost:5432`.

### Option 2 — Native Installation

1. Install [PostgreSQL 16](https://www.postgresql.org/download/) for your OS.
2. Start the service and create the database:

```sql
CREATE DATABASE ecommerce_analytics_platform_db;
```

3. Update the connection details in `backend/ecommerce-backend/src/main/resources/application.properties` and `Text to SQL Agent/.env` to match your local credentials.

---

## Notes

- The `etl/` subdirectory contains an older MySQL-based ETL pipeline. It is **deprecated** and should not be used.
- Because Hibernate manages the schema, never manually `DROP` or `ALTER` production tables — update the Java entity and let Hibernate apply the change on next startup.
- The `audit_logs` and `chat_histories` tables store `user_id` as a plain integer without a foreign key constraint, allowing records to persist even if the referenced user is deleted.
