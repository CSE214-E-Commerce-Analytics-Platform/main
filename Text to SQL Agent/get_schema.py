from tools.db_executor import get_connection

def update_schema():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE table_schema='public'
    """)
    rows = cur.fetchall()
    
    schema = {}
    for table, col in rows:
        if table not in schema:
            schema[table] = []
        schema[table].append(col)
        
    schema_str = ""
    for table, cols in schema.items():
        schema_str += f"{table.upper()} ({', '.join(cols)})\n"
        
    with open("schema_clean.txt", "w", encoding="utf-8") as f:
        f.write(schema_str)
    
    print("Schema dumped to schema_clean.txt")

if __name__ == "__main__":
    update_schema()
