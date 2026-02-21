import pandas as pd
from sqlalchemy import create_engine, text
import random

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def run_reviews_etl(file_path):
    print("⭐ Ürün yorumları işleniyor...")
    # Gerekli sütunları oku
    df = pd.read_csv(file_path)
    
    with engine.connect() as conn:
        # Mevcut INDIVIDUAL kullanıcıları al
        user_ids = [row[0] for row in conn.execute(text("SELECT id FROM users WHERE role_type = 'INDIVIDUAL'")).fetchall()]
        
        if not user_ids:
            print("❌ Hata: Yorum yapacak kullanıcı bulunamadı!")
            return

        count = 0
        for index, row in df.iterrows():
            try:
                # 1. Ürünü SKU (product_id) üzerinden bul
                # Amazon'daki product_id senin veritabanındaki SKU'dur
                product_res = conn.execute(
                    text("SELECT id FROM products WHERE sku = :sku LIMIT 1"), 
                    {"sku": row['product_id']}
                ).fetchone()

                if product_res:
                    product_id = product_res[0]
                    random_user_id = random.choice(user_ids)
                    
                    # 2. Rating'e göre basit sentiment belirle
                    try:
                        score = float(str(row['rating']).replace('|', '5')) # Hatalı verileri temizle
                        sentiment = "POSITIVE" if score >= 4 else "NEUTRAL" if score >= 3 else "NEGATIVE"
                    except:
                        sentiment = "POSITIVE"

                    # 3. REVIEWS Tablosuna Ekle
                    # Sütun isimleri diyagramına göre: user_id, product_id, star_rating, sentiment, comment_text
                    review_sql = text("""
                        INSERT INTO reviews (user_id, product_id, star_rating, sentiment, comment_text)
                        VALUES (:u_id, :p_id, :rating, :sent, :txt)
                    """)
                    
                    conn.execute(review_sql, {
                        "u_id": random_user_id,
                        "p_id": product_id,
                        "rating": score,
                        "sent": sentiment,
                        "txt": row['review_content'][:500] # Çok uzun yorumları keselim
                    })
                    
                    conn.commit()
                    count += 1
                    
                    if count % 100 == 0:
                        print(f"✅ {count} adet yorum yüklendi...")
                
                # Toplam 5.000 yorum yeterli olacaktır
                if count >= 5000:
                    break

            except Exception as e:
                print(f"⚠️ Satır {index} işlenirken hata: {e}")
                continue

    print(f"🚀 İşlem bitti! Toplam {count} ürün yorumu oluşturuldu.")

# Dosya adını kontrol et: 'amazon_sales_dataset.csv' mi?
run_reviews_etl('../datasets/amazon_sales.csv')