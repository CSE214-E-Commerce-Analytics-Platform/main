import pandas as pd
from sqlalchemy import create_engine, text
import random

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def run_shipments_etl(file_path):
    print("🚚 Sevkiyat verileri (ecommerce_shipping.csv) işleniyor...")
    df = pd.read_csv(file_path)
    
    with engine.connect() as conn:
        # Mevcut sipariş ID'lerini alalım (Sevkiyatları gerçek siparişlerle bağlamak için)
        order_ids = [row[0] for row in conn.execute(text("SELECT id FROM orders")).fetchall()]
        
        if not order_ids:
            print("❌ Hata: Veritabanında sipariş bulunamadı! Önce orders tablosunu doldurmalısın.")
            return

        print(f"📦 Toplam {len(order_ids)} sipariş üzerinden sevkiyatlar dağıtılacak.")

        count = 0
        for index, row in df.iterrows():
            try:
                # Rastgele bir sipariş ID'si seç (veya sırayla ata)
                # Not: Her siparişin bir sevkiyatı olduğunu varsayıyoruz
                if count >= len(order_ids):
                    break
                
                current_order_id = order_ids[count]
                
                # Reached.on.Time_Y.N: 1 ise geç kalmış, 0 ise zamanında ulaşmış
                # Bunu metin olarak status'e çevirelim
                shipment_status = "DELAYED" if row['Reached.on.Time_Y.N'] == 1 else "ON_TIME"
                
                # SHIPMENTS Tablosuna Ekle (Diyagram: order_id, warehouse, mode, status)
                ship_sql = text("""
                    INSERT INTO shipments (order_id, warehouse, mode, status)
                    VALUES (:o_id, :wh, :mode, :stat)
                """)
                
                conn.execute(ship_sql, {
                    "o_id": current_order_id,
                    "wh": row['Warehouse_block'],
                    "mode": row['Mode_of_Shipment'],
                    "stat": shipment_status
                })
                
                count += 1
                if count % 500 == 0:
                    conn.commit()
                    print(f"✅ {count} sevkiyat kaydı oluşturuldu...")

            except Exception as e:
                print(f"⚠️ Satır {index} işlenirken hata: {e}")
                continue
        
        conn.commit()
    print(f"🚀 İşlem bitti! Toplam {count} sevkiyat kaydı MySQL'e eklendi.")

# Dosya adının tam olarak 'ecommerce_shipping.csv' olduğundan emin ol
run_shipments_etl('../datasets/e-commerce_shipping.csv')