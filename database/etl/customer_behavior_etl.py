import pandas as pd
from sqlalchemy import create_engine, text

# 1. Veritabanı Bağlantısı
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/ecommerce_analytics_platform_db')

def run_customer_behavior_etl(file_path):
    print("👥 Veri yükleme denemesi başlıyor...")
    df = pd.read_csv(file_path)
    
    with engine.connect() as conn:
        for index, row in df.iterrows():
            try:
                # E-posta oluştur
                user_email = f"user_{row['Customer ID']}@example.com"
                
                # USERS EKLEME
                # Buraya role_type'ı doğrudan metin olarak gömüyoruz
                user_sql = text("""
                    INSERT INTO users (email, password_hash, role_type, gender)
                    VALUES (:email, :pw, 'INDIVIDUAL', :gender)
                    ON DUPLICATE KEY UPDATE gender = :gender
                """)
                
                conn.execute(user_sql, {
                    "email": user_email,
                    "pw": "pbkdf2:sha256:user_secret_123",
                    "gender": str(row['Gender']).upper()
                })

                # ID ALMA
                user_id = conn.execute(
                    text("SELECT id FROM users WHERE email = :email"), 
                    {"email": user_email}
                ).fetchone()[0]

                # PROFIL EKLEME
                profile_sql = text("""
                    INSERT INTO customers_profiles (user_id, age, city, membership_type)
                    VALUES (:u_id, :age, :city, :m_type)
                    ON DUPLICATE KEY UPDATE age=:age, city=:city, membership_type=:m_type
                """)
                
                conn.execute(profile_sql, {
                    "u_id": user_id,
                    "age": row['Age'],
                    "city": row['City'],
                    "m_type": row['Membership Type']
                })
                
                conn.commit()
                if index % 50 == 0: print(f"✅ {index} satır tamamlandı...")

            except Exception as e:
                # HATA BURADA BELLİ OLACAK
                print(f"❌ {index}. Satırda DURDURAN Hata: {e}")
                return # İlk hatada dur ki ne olduğunu görelim!

    print("🚀 İşlem başarıyla bitti.")

run_customer_behavior_etl('../datasets/E-commerce_Customer_Behavior.csv')