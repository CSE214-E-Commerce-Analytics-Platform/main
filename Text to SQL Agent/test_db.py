from tools.db_executor import get_connection

try:
    conn = get_connection()
    print("Supabase bağlantısı başarılı!")
    conn.close()
except Exception as e:
    print("Hata:", e)