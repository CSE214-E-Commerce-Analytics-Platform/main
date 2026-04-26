#!/usr/bin/env python3
"""
CSV ETL — adds real-world data on top of existing DB (ADDITIVE, no deletes).

Sources used:
  amazon_sales.csv          → products (real names/categories) + reviews (real text)
  E-commerce_Customer_Behavior.csv → individual users with real demographics

Sources SKIPPED:
  e-commerce_shipping.csv   → no linkable order_id, would break master-child structure
  ecommerce_user_dataset.csv → abstract segmentation, not mappable

Run AFTER seed.py (needs existing stores + categories):
    python etl_csv.py

Env vars (same as seed.py):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLMODE
"""

import csv
import os
import re
import random
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta

import bcrypt
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)
random.seed(99)

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR     = r"C:\Users\Furkan\Desktop\CSE214_Project_Gereksinimler\datasets"
AMAZON_CSV   = os.path.join(BASE_DIR, "amazon_sales.csv")
BEHAVIOR_CSV = os.path.join(BASE_DIR, "E-commerce_Customer_Behavior.csv")

# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "ecommerce_analytics_platform_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
    )
    conn.set_client_encoding('UTF8')
    return conn

PASSWORD_HASH = bcrypt.hashpw(b"password123", bcrypt.gensalt(10)).decode()

# ── Category → image (keyword-based, best-effort for Amazon categories) ───────
# Amazon categories don't match ours exactly, so we do keyword matching on the
# raw category string. First match wins. Falls back to cat-general.jpg.

_CAT_IMAGE_RULES = [
    (["phone", "smartphone", "mobile", "iphone", "samsung", "android"],  "categories/cat-smartphones.webp"),
    (["laptop", "notebook", "macbook", "computer", "pc", "desktop"],      "categories/cat-electronics.jfif"),
    (["headphone", "earphone", "audio", "speaker", "sound"],              "categories/cat-electronics.jfif"),
    (["tablet", "ipad"],                                                   "categories/cat-electronics.jfif"),
    (["cable", "usb", "charger", "adapter", "accessories", "peripheral"], "categories/cat-electronics.jfif"),
    (["camera", "photo", "lens", "gopro"],                                "categories/cat-electronics.jfif"),
    (["television", "tv", "monitor", "display", "screen"],                "categories/cat-electronics.jfif"),
    (["clothing", "apparel", "fashion", "shirt", "dress", "wear"],        "categories/cat-apparel.jfif"),
    (["shoe", "boot", "sneaker", "footwear"],                             "categories/cat-apparel.jfif"),
    (["book", "novel", "guide", "manual"],                                "categories/cat-books-media.jfif"),
    (["kitchen", "cook", "food", "beverage", "coffee"],                   "categories/cat-kitchen-appliances.jfif"),
    (["furniture", "chair", "desk", "sofa", "table"],                     "categories/cat-furniture.jfif"),
    (["fitness", "gym", "sport", "yoga", "exercise", "dumbbell"],         "categories/cat-sports.jfif"),
    (["beauty", "skin", "care", "cosmetic", "makeup"],                    "categories/cat-skincare.jfif"),
    (["garden", "plant", "outdoor", "tool"],                              "categories/cat-garden-tools.jfif"),
    (["light", "lamp", "led", "bulb"],                                    "categories/cat-lighting.jfif"),
    (["supplement", "vitamin", "health", "protein"],                      "categories/cat-supplements.jfif"),
]

def _image_for_amazon_category(raw_category: str) -> str:
    key = raw_category.lower()
    for keywords, image in _CAT_IMAGE_RULES:
        if any(kw in key for kw in keywords):
            return image
    return "categories/cat-general.jfif"

# ── Helpers ───────────────────────────────────────────────────────────────────

def rdate(days_ago_min=1, days_ago_max=365):
    delta = random.randint(days_ago_min, days_ago_max)
    return datetime.now() - timedelta(days=delta, seconds=random.randint(0, 86400))


def clean_price(raw: str) -> Decimal | None:
    """Strip ₹ / , / spaces and return Decimal, or None if unparseable."""
    cleaned = re.sub(r"[₹,\s]", "", raw.strip())
    try:
        val = Decimal(cleaned)
        # Clamp to reasonable TRY range (treat ₹ value as ₺ directly)
        return max(Decimal("9.99"), min(val, Decimal("999999.99")))
    except InvalidOperation:
        return None


def parse_amazon_category(raw: str) -> tuple[str, str]:
    """
    'Computers&Accessories|Accessories&Peripherals|Cables|USBCables'
    → parent='Computers & Accessories', leaf='USBCables'
    """
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    if not parts:
        return "General", "General"
    parent = parts[0].replace("&", " & ")
    leaf   = parts[-1].replace("&", " & ") if len(parts) > 1 else parent
    return parent[:255], leaf[:255]


def get_or_create_category(cur, name: str, parent_name: str | None = None) -> int:
    """Return category id, creating if needed."""
    parent_id = None
    if parent_name:
        cur.execute("SELECT id FROM categories WHERE name = %s LIMIT 1", (parent_name,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO categories (name, created_at, updated_at) VALUES (%s, NOW(), NOW()) RETURNING id",
                (parent_name,)
            )
            parent_id = cur.fetchone()[0]
        else:
            parent_id = row[0]

    cur.execute(
        "SELECT id FROM categories WHERE name = %s AND (parent_id = %s OR (parent_id IS NULL AND %s IS NULL)) LIMIT 1",
        (name, parent_id, parent_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO categories (name, parent_id, created_at, updated_at) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
        (name, parent_id)
    )
    return cur.fetchone()[0]


# ── ETL 1: Amazon products + reviews ─────────────────────────────────────────

def etl_amazon(conn, cur):
    print("  📦 amazon_sales.csv → products + reviews...")

    # Load existing stores
    cur.execute("SELECT id FROM stores")
    store_ids = [r[0] for r in cur.fetchall()]
    if not store_ids:
        print("      ⚠️  No stores found — skipping amazon ETL")
        return

    # Load existing individual user ids for reviews
    cur.execute("SELECT id FROM users WHERE role_type = 'INDIVIDUAL'")
    ind_user_ids = [r[0] for r in cur.fetchall()]
    if not ind_user_ids:
        print("      ⚠️  No individual users — reviews will be skipped")

    # Load existing (user_id, product_id) review pairs to avoid duplicates
    cur.execute("SELECT user_id, product_id FROM reviews")
    existing_reviews = set(tuple(r) for r in cur.fetchall())

    products_added = 0
    reviews_added  = 0
    seen_skus      = set()

    with open(AMAZON_CSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # ── Product ──────────────────────────────────────────────────────
            raw_name  = (row.get("product_name") or "").strip()
            raw_price = (row.get("discounted_price") or row.get("actual_price") or "").strip()
            raw_cat   = (row.get("category") or "").strip()
            raw_asin  = (row.get("product_id") or "").strip()

            if not raw_name or not raw_asin:
                continue

            # Truncate long product names
            product_name = raw_name[:240]
            sku          = f"AMZN-{raw_asin[:30]}"

            if sku in seen_skus:
                continue
            seen_skus.add(sku)

            price = clean_price(raw_price)
            if price is None:
                price = Decimal("199.99")

            parent_cat_name, leaf_cat_name = parse_amazon_category(raw_cat)
            cat_id = get_or_create_category(cur, leaf_cat_name, parent_cat_name)

            store_id = random.choice(store_ids)
            created   = rdate(100, 500)
            image_url = _image_for_amazon_category(raw_cat)

            cur.execute(
                "INSERT INTO products (name, sku, unit_price, stock_quantity, store_id, category_id, "
                "description, image_url, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (sku) DO NOTHING RETURNING id",
                (product_name, sku, price, random.randint(10, 500),
                 store_id, cat_id,
                 f"Imported from Amazon marketplace. Category: {leaf_cat_name}."[:250],
                 image_url, created, created)
            )
            product_row = cur.fetchone()
            if not product_row:
                # Already exists — fetch its id for reviews
                cur.execute("SELECT id FROM products WHERE sku = %s", (sku,))
                product_row = cur.fetchone()
            if not product_row:
                continue

            product_id = product_row[0]
            products_added += 1

            # ── Reviews (from review_content column) ─────────────────────────
            if not ind_user_ids:
                continue

            raw_rating  = (row.get("rating") or "").strip()
            raw_content = (row.get("review_content") or "").strip()

            # rating is avg (e.g. 4.2) → round to int for star_rating
            try:
                avg_rating = float(raw_rating)
                base_star  = round(avg_rating)
                base_star  = max(1, min(5, base_star))
            except (ValueError, TypeError):
                base_star = 3

            # Split review_content by comma (each is an individual review)
            review_texts = [t.strip() for t in raw_content.split(",") if len(t.strip()) > 10]

            for text in review_texts[:4]:  # max 4 reviews per product
                uid = random.choice(ind_user_ids)
                if (uid, product_id) in existing_reviews:
                    continue
                existing_reviews.add((uid, product_id))

                # Slight star variance per review
                star = max(1, min(5, base_star + random.choice([-1, 0, 0, 1])))
                sentiment = "POSITIVE" if star >= 4 else ("NEGATIVE" if star <= 2 else "NEUTRAL")
                rev_date  = rdate(0, 300)

                cur.execute(
                    "INSERT INTO reviews (product_id, user_id, star_rating, sentiment, comment_text, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (product_id, uid, star, sentiment, text[:1000], rev_date, rev_date)
                )
                reviews_added += 1

            if products_added % 100 == 0:
                conn.commit()

    conn.commit()
    print(f"      ✓ {products_added} products, {reviews_added} reviews added from Amazon CSV")


# ── ETL 2: Customer Behavior → individual users + profiles ───────────────────

def etl_customer_behavior(conn, cur):
    print("  👥 E-commerce_Customer_Behavior.csv → users + profiles...")

    # Membership mapping: Gold→2(VIP) Silver→1(PREMIUM) Bronze→0(STANDARD)
    membership_map = {"gold": 2, "silver": 1, "bronze": 0}

    users_added    = 0
    profiles_added = 0
    seen_emails    = set()

    with open(BEHAVIOR_CSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            cid          = (row.get("Customer ID") or "").strip()
            age_raw      = (row.get("Age") or "").strip()
            city         = (row.get("City") or "Unknown")[:100]
            mem_raw      = (row.get("Membership Type") or "Bronze").strip().lower()
            satisfaction = (row.get("Satisfaction Level") or "").strip()

            try:
                age = int(age_raw)
                age = max(18, min(90, age))
            except ValueError:
                age = 30

            membership_int = membership_map.get(mem_raw, 0)

            # Create a synthetic but unique email
            email = f"cust.behavior.{cid.lower()}@demo.com"
            if email in seen_emails:
                continue
            seen_emails.add(email)

            created = rdate(0, 500)
            cur.execute(
                "INSERT INTO users (email, password_hash, provider, role_type, is_active, created_at, updated_at) "
                "VALUES (%s, %s, 'LOCAL', 'INDIVIDUAL', true, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
                (email, PASSWORD_HASH, created, created)
            )
            user_row = cur.fetchone()
            if not user_row:
                cur.execute("SELECT id FROM users WHERE email = %s", (email,))
                user_row = cur.fetchone()
            if not user_row:
                continue

            uid = user_row[0]
            users_added += 1

            # Create profile
            cur.execute(
                "INSERT INTO customer_profiles (user_id, age, city, state, country, membership_type, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, 'USA', %s, NOW(), NOW()) ON CONFLICT (user_id) DO NOTHING",
                (uid, age, city, city, membership_int)
            )
            profiles_added += 1

            # Create cart (required one-to-one)
            cur.execute(
                "INSERT INTO carts (user_id, total_price, created_at, updated_at) "
                "VALUES (%s, 0.00, NOW(), NOW()) ON CONFLICT (user_id) DO NOTHING",
                (uid,)
            )

            # Create 1 address per user so they can have orders later
            cur.execute(
                "SELECT id FROM addresses WHERE user_id = %s LIMIT 1", (uid,)
            )
            if not cur.fetchone():
                cities = ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]
                c = random.choice(cities)
                cur.execute(
                    "INSERT INTO addresses (user_id, city, district, full_address, phone_number, zip_code, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())",
                    (uid, city, c,
                     f"{random.randint(1, 999)} {c} Street, Apt {random.randint(1, 50)}",
                     f"+1555{random.randint(1000000, 9999999)}", f"{random.randint(10000, 99999)}")
                )

            if users_added % 50 == 0:
                conn.commit()

    conn.commit()
    print(f"      ✓ {users_added} users, {profiles_added} profiles added from Behavior CSV")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n📂 Starting CSV ETL (additive — no data deleted)...\n")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        etl_amazon(conn, cur)
        etl_customer_behavior(conn, cur)

        print("\n✅ CSV ETL complete!")
        print("   Tip: run seed.py first if you haven't already.\n")

    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        print(f"\n❌ ETL failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
