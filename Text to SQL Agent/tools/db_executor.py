import psycopg2
import psycopg2.extras
import os
import re
from dotenv import load_dotenv
load_dotenv()

BLOCKED_COLUMNS = {
    "password_hash", "token", "replaced_by", "revoked_at",
    "internal_cost", "api_key", "supplier_margin", "cost_price",
    "password", "secret", "private_key", "refresh_token"
}

ALLOWED_COLUMNS = {
    "orders": ["id", "status", "grand_total", "created_at", "store_id", "user_id"],
    "products": ["id", "name", "sku", "unit_price", "category_id", "store_id"],
    "users": ["id", "email", "role_type", "gender"],
    "reviews": ["id", "star_rating", "sentiment", "product_id", "user_id"],
    "shipments": ["id", "order_id", "warehouse", "mode", "status"],
}

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require"   # Supabase için şart
    )

def sanitize_result(rows: list[dict]) -> list[dict]:
    return [
        {k: v for k, v in row.items() if k.lower() not in BLOCKED_COLUMNS}
        for row in rows
    ]

def execute_query(sql: str, user_role: str, user_id: int, store_id: int = None) -> str:
    sql_upper = sql.strip().upper()

    # Only SELECT is allowed
    if not sql_upper.startswith("SELECT"):
        return "ERROR: Only SELECT queries are permitted."

    # Block dangerous keywords (Use \b for exact word match so 'updated_at' is not blocked as 'UPDATE')
    dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "ALTER", "EXEC"]
    for keyword in dangerous:
        if re.search(r'\b' + keyword + r'\b', sql_upper):
            return f"ERROR: '{keyword}' command is not permitted."

    # Layer 5 — Block queries referencing sensitive columns (AV-12)
    for blocked_col in BLOCKED_COLUMNS:
        if blocked_col in sql.lower():
            return f"ERROR: Access to column '{blocked_col}' is not permitted."

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            return "No results found."

        result = [dict(zip(columns, row)) for row in rows[:50]]
        sanitized = sanitize_result(result)
        return str(sanitized)

    except Exception as e:
        return f"DB_ERROR: {str(e)}"