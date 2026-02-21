import pandas as pd
from sqlalchemy import create_engine, text
import random

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def run_orders_etl(file_path):
    print("📦 Sipariş verileri işleniyor...")
    # Sadece gerekli kolonları yükleyerek hafızayı koruyalım
    cols = ['increment_id', 'status', 'created_at', 'sku', 'price', 'qty_ordered', 'grand_total']
    df = pd.read_csv(file_path, usecols=cols, low_memory=False)
    
    # Boş verileri temizle
    df = df.dropna(subset=['increment_id', 'sku'])

    with engine.connect() as conn:
        # A. INDIVIDUAL kullanıcıların ID'lerini al
        user_ids = [row[0] for row in conn.execute(text("SELECT id FROM users WHERE role_type = 'INDIVIDUAL'")).fetchall()]
        
        if not user_ids:
            print("❌ Hata: Veritabanında INDIVIDUAL kullanıcı bulunamadı!")
            return

        print(f"👥 {len(user_ids)} kullanıcı üzerinden siparişler dağıtılacak.")

        # B. Siparişleri gruplayalım (Çünkü bir siparişte birden fazla ürün olabilir)
        grouped_orders = df.groupby('increment_id')
        
        count = 0
        for inc_id, group in grouped_orders:
            try:
                # Rastgele bir kullanıcı seç
                random_user_id = random.choice(user_ids)
                
                # İlk satırdan sipariş genel bilgilerini al
                first_row = group.iloc[0]
                
                # 1. ORDERS Tablosuna Ekle
                # Not: store_id'yi 1 (senin eklediğin depo) olarak varsayıyoruz
                order_sql = text("""
                    INSERT INTO orders (user_id, store_id, grand_total, status, order_date)
                    VALUES (:u_id, 1, :total, :status, :c_at)
                """)
                
                # Tarih formatını MySQL'e uygun hale getir (7/1/2016 -> 2016-07-01)
                order_date = pd.to_datetime(first_row['created_at']).strftime('%Y-%m-%d %H:%M:%S')
                
                result = conn.execute(order_sql, {
                    "u_id": random_user_id,
                    "total": first_row['grand_total'],
                    "status": str(first_row['status']).upper(),
                    "c_at": order_date
                })
                
                # Yeni oluşan Order ID'yi al
                order_id = result.lastrowid

                # 2. ORDER_ITEMS Tablosuna Siparişteki Tüm Ürünleri Ekle
                for _, item_row in group.iterrows():
                    # SKU üzerinden Product ID bulmaya çalış (Eşleşmezse hata almamak için)
                    product_res = conn.execute(
                        text("SELECT id FROM products WHERE sku = :sku LIMIT 1"), 
                        {"sku": item_row['sku']}
                    ).fetchone()
                    
                    if product_res:
                        product_id = product_res[0]
                        
                        item_sql = text("""
                            INSERT INTO order_items (order_id, product_id, quantity, price)
                            VALUES (:o_id, :p_id, :qty, :price)
                        """)
                        conn.execute(item_sql, {
                            "o_id": order_id,
                            "p_id": product_id,
                            "qty": item_row['qty_ordered'],
                            "price": item_row['price']
                        })
                
                conn.commit()
                count += 1
                
                if count % 100 == 0:
                    print(f"✅ {count} adet sipariş ve detayları işlendi...")
                
                # Çok fazla beklememek için 10.000 siparişte durabiliriz
                if count >= 50000:
                    break

            except Exception as e:
                conn.rollback()
                print(f"⚠️ Sipariş {inc_id} işlenirken hata: {e}")
                continue

    print(f"🚀 İşlem bitti! Toplam {count} sipariş oluşturuldu.")

# Çalıştır
run_orders_etl('../datasets/Pakistan_Largest_Ecommerce_Dataset.csv')