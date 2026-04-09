import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
load_dotenv()

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

def execute_query(sql: str, user_role: str, user_id: int, store_id: int = None) -> str:
    sql_upper = sql.strip().upper()
    
    # Sadece SELECT izin ver
    if not sql_upper.startswith("SELECT"):
        return "ERROR: Sadece SELECT sorguları çalıştırılabilir."
    
    # Tehlikeli keywordleri engelle
    dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "ALTER", "EXEC"]
    for keyword in dangerous:
        if keyword in sql_upper:
            return f"ERROR: '{keyword}' komutu engellendi."

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            return "Sonuç bulunamadı."
        
        # Kolon isimlerini de döndür
        result = [dict(zip(columns, row)) for row in rows[:50]]
        return str(result)
        
    except Exception as e:
        return f"DB_ERROR: {str(e)}"