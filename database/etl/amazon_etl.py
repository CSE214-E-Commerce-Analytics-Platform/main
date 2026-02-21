import pandas as pd
from sqlalchemy import create_engine, text
import re

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def clean_price(value):
    """'â‚¹1,099' gibi bozuk sembolleri ve virgülleri temizleyip float yapar."""
    if pd.isna(value) or value == '':
        return 0.0
    # Sadece rakamları ve noktayı tut (RegEx kullanarak temizlik)
    cleaned = re.sub(r'[^\d.]', '', str(value))

    cleaned = cleaned.replace(',', '')

    try:
        return float(cleaned)
    except:
        return 0.0

def get_or_create_category(cat_name, parent_id=None):
    """Kategori hiyerarşisini kontrol eder veya oluşturur."""
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

def run_amazon_etl(file_path):
    # Encoding hatasını önlemek için utf-8-sig veya latin-1 denenebilir
    df = pd.read_csv(file_path, encoding='utf-8')
    
    # Veri Temizleme
    df['discounted_price'] = df['discounted_price'].apply(clean_price)
    df['actual_price'] = df['actual_price'].apply(clean_price)
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            # A. Kategori Parçalama: Computers&Accessories|Accessories&Peripherals...
            raw_cats = str(row['category']).split('|')
            curr_parent_id = None
            for cat in raw_cats:
                curr_parent_id = get_or_create_category(cat.strip(), curr_parent_id)
            
            # B. Ürün Ekleme (Products Tablosu)
            prod_query = text("""
                INSERT INTO products (sku, name, category_id, unit_price, store_id)
                VALUES (:sku, :name, :cat_id, :price, :store_id)
                ON DUPLICATE KEY UPDATE unit_price = VALUES(unit_price)
            """)
            conn.execute(prod_query, {
                "sku": row['product_id'], # Ör: B07JW9H4J1
                "name": row['product_name'][:255],
                "cat_id": curr_parent_id,
                "price": row['discounted_price'],
                "store_id": 1 # Varsayılan ana mağaza
            })
            conn.commit()
    print("🚀 Veriler başarıyla temizlendi ve MySQL'e aktarıldı!")

run_amazon_etl('../datasets/amazon_sales.csv')