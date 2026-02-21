import pandas as pd
from sqlalchemy import create_engine, text

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def clean_pakistan_price(value):
    """Fiyatı temizleyip float yapar."""
    if pd.isna(value) or value == '':
        return 0.0
    try:
        # Virgül veya boşluk varsa temizle
        cleaned = str(value).replace(',', '').replace(' ', '').strip()
        return float(cleaned)
    except:
        return 0.0

def get_or_create_category(cat_name, parent_id=None):
    """Kategori varsa ID döner, yoksa oluşturur."""
    if not cat_name or cat_name == r'\N': return None
    
    with engine.connect() as conn:
        query = text("SELECT id FROM categories WHERE name = :name AND (parent_id <=> :parent_id)")
        result = conn.execute(query, {"name": cat_name, "parent_id": parent_id}).fetchone()
        
        if result:
            return result[0]
        else:
            ins_query = text("INSERT INTO categories (name, parent_id) VALUES (:name, :parent_id)")
            conn.execute(ins_query, {"name": cat_name, "parent_id": parent_id})
            conn.commit()
            return conn.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]

def run_pakistan_etl(file_path):
    print("🇵🇰 Pakistan veri seti okunuyor...")
    # Dosya çok büyükse low_memory=False hatayı önler
    df = pd.read_csv(file_path, low_memory=False)
    
    print("🚀 Veri aktarımı başladı...")
    with engine.connect() as conn:
        for _, row in df.iterrows():
            # 1. Kategori İşleme (Tek katmanlı)
            cat_name = str(row['category_name_1']).strip()
            cat_id = get_or_create_category(cat_name)
            
            # 2. Ürün Ekleme (Products)
            # Pakistan verisinde SKU bazen boş gelebilir (\N)
            sku = str(row['sku']).strip()
            if sku == r'\N' or sku == 'nan':
                sku = f"PAK-TMP-{row['item_id']}" # Geçici SKU üret

            prod_query = text("""
                INSERT INTO products (sku, name, category_id, unit_price, store_id)
                VALUES (:sku, :name, :cat_id, :price, :store_id)
                ON DUPLICATE KEY UPDATE unit_price = VALUES(unit_price)
            """)
            
            conn.execute(prod_query, {
                "sku": sku,
                "name": sku, # Pakistan verisinde ürün adı ayrı değil, SKU'yu isim olarak kullanıyoruz
                "cat_id": cat_id,
                "price": clean_pakistan_price(row['price']),
                "store_id": 1 # Varsayılan Mağaza
            })
            conn.commit()
            
    print("✅ Pakistan verileri başarıyla sisteme eklendi!")

# Çalıştırmak için:
run_pakistan_etl('../datasets/Pakistan_Largest_Ecommerce_Dataset.csv')