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
| Password | `admin` (default) | set via `DB_PASS` env var |

---

## ER Diagram

The full ER diagram is available as a PDF in this directory: [`ecommerce_platform_db_diagram.pdf`](./ecommerce_platform_db_diagram.pdf).

---

## Schema Overview

The database contains **17 tables** organised around the core e-commerce domains:

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

### `users`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | Auto-increment |
| email | VARCHAR UNIQUE NOT NULL | |
| password_hash | VARCHAR | BCrypt |
| provider | VARCHAR | STRING enum: `LOCAL`, `GOOGLE` |
| role_type | VARCHAR | STRING enum: `INDIVIDUAL`, `CORPORATE`, `ADMIN` |
| is_active | BOOLEAN | default `true` |
| created_at / updated_at | TIMESTAMP | |

### `stores`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| name | VARCHAR NOT NULL | |
| status | VARCHAR | e.g. `ACTIVE` |
| owner_id | BIGINT UNIQUE NOT NULL | FK → users, strict 1:1 |

### `products`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| name, description | VARCHAR / TEXT | |
| image_url | VARCHAR | Format: `cat-{slug}.jfif` or `.webp` |
| sku | VARCHAR UNIQUE NOT NULL | |
| unit_price | DECIMAL | |
| stock_quantity | INTEGER | |
| store_id | BIGINT NOT NULL | FK → stores |
| category_id | BIGINT NULL | FK → categories |

### `orders`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| status | VARCHAR | STRING enum — see above |
| grand_total | DECIMAL(10,2) | |
| order_date | TIMESTAMP | |
| parent_order_id | BIGINT NULL | FK → orders (self); NULL = top-level order |
| store_id | BIGINT NULL | FK → stores; NULL = parent/master order |
| user_id | BIGINT NOT NULL | FK → users |
| address_id | BIGINT NOT NULL | FK → addresses |

### `payments`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| order_id | BIGINT UNIQUE NOT NULL | FK → orders, 1:1 |
| amount | DECIMAL(18,2) NOT NULL | |
| payment_method | VARCHAR | STRING enum |
| status | VARCHAR | STRING enum |
| transaction_key | VARCHAR | UUID |
| error_message | TEXT NULL | Populated on failure |

### `shipments`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| warehouse | VARCHAR | e.g. `Istanbul-Main` |
| tracking_number | VARCHAR | |
| mode | VARCHAR | `Standard`, `Express`, `Same Day` |
| status | SMALLINT | ORDINAL enum — see above |
| estimated_delivery_date | TIMESTAMP | |
| order_id | BIGINT NOT NULL | FK → orders |

### `reviews`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| star_rating | INTEGER NOT NULL | 1–5 |
| sentiment | VARCHAR(50) | STRING enum |
| comment_text | TEXT | |
| product_id | BIGINT NOT NULL | FK → products |
| user_id | BIGINT NOT NULL | FK → users |

### `customer_profiles`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| user_id | BIGINT UNIQUE NOT NULL | FK → users, 1:1 |
| age | INT | |
| city / state / country | VARCHAR | |
| membership_type | SMALLINT | ORDINAL enum: 0=STANDARD, 1=PREMIUM, 2=VIP |

### `chat_histories`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | |
| title | VARCHAR NOT NULL | First 50 chars of the initial query |
| initial_query | TEXT NOT NULL | Full first question |
| user_id | BIGINT NOT NULL | Denormalized (no FK constraint) |

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
