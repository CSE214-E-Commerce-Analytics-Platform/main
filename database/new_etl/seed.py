#!/usr/bin/env python3
"""
E-Commerce Analytics Platform — Database Seeder
Adds realistic fake data ON TOP of existing data (does not wipe anything).

Key schema notes from the actual DB dump:
  - shipments.status         → smallint  0=PENDING 1=LABEL_CREATED 2=IN_TRANSIT
                                          3=OUT_FOR_DELIVERY 4=DELIVERED 5=RETURNED 6=CANCELLED
  - customer_profiles.membership_type → smallint  0=STANDARD 1=PREMIUM 2=VIP
  - payments has UNIQUE(order_id)  → one payment per order only
  - carts    has UNIQUE(user_id)   → one cart per user only
  - stores   has UNIQUE(owner_id)  → one store per corporate user only

Image convention:
  Products use their category's representative image.
  Files live in the frontend's assets/images/ folder.
  Format: cat-{slug}.jpg  (all lowercase, spaces→hyphens)

Usage:
    pip install -r requirements.txt
    python seed.py
"""

import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import bcrypt
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv(override=True)

fake = Faker("en_US")
random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────

NUM_EXTRA_INDIVIDUAL_USERS = 50
NUM_ORDERS                 = 300
NUM_REVIEWS                = 200

PASSWORD_HASH = bcrypt.hashpw(b"password123", bcrypt.gensalt(10)).decode()

# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "ecommerce_analytics_platform_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def rdate(days_ago_min=1, days_ago_max=365):
    delta = random.randint(days_ago_min, days_ago_max)
    return datetime.now() - timedelta(days=delta, seconds=random.randint(0, 86400))

def wchoice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]

# ── Category → Image mapping ──────────────────────────────────────────────────
# Each category has one representative image file.
# Files must be placed in: frontend/e-commerce-frontend/src/assets/images/categories/
# Naming format: cat-{slug}.jpg  (lowercase, spaces→hyphens)

CAT_IMAGE = {
    # ── Existing DB categories (Bob's Tech World + Alice Fashion's World) ──
    "Electronics":          "cat-electronics.jfif",
    "Apparel":              "cat-apparel.jfif",
    "Smarthphone":          "cat-smartphones.webp",   # note: typo from original DB
    "Smartphone":           "cat-smartphones.webp",
    "T-Shirt":              "cat-t-shirt.jfif",
    "Sports":               "cat-sports.jfif",

    # ── New parent categories ──
    "Home & Garden":        "cat-home-garden.jfif",
    "Sports & Outdoors":    "cat-sports-outdoors.jfif",
    "Books & Media":        "cat-books-media.jfif",
    "Food & Beverages":     "cat-food-beverages.jfif",
    "Beauty & Health":      "cat-beauty-health.jfif",

    # ── HomeStyle leaf categories ──
    "Furniture":            "cat-furniture.jfif",
    "Kitchen Appliances":   "cat-kitchen-appliances.jfif",
    "Garden Tools":         "cat-garden-tools.jfif",
    "Lighting":             "cat-lighting.jfif",

    # ── SportZone leaf categories ──
    "Fitness Equipment":    "cat-fitness-equipment.jfif",
    "Outdoor Gear":         "cat-outdoor-gear.jfif",
    "Team Sports":          "cat-team-sports.jfif",

    # ── BookCorner leaf categories ──
    "Programming":          "cat-programming.jfif",
    "Business":             "cat-business-books.jfif",
    "Fiction":              "cat-fiction.jfif",
    "Art Supplies":         "cat-art-supplies.jfif",

    # ── Extra leaf categories ──
    "Organic Food":         "cat-organic-food.jfif",
    "Beverages":            "cat-beverages.jfif",
    "Snacks":               "cat-snacks.jfif",
    "Skincare":             "cat-skincare.jfif",
    "Supplements":          "cat-supplements.jfif",
    "Hair Care":            "cat-hair-care.jfif",
}

DEFAULT_IMAGE = "cat-general.jfif"   # fallback if category not in map

# ── New categories to add ─────────────────────────────────────────────────────

EXTRA_CATEGORIES = {
    "Home & Garden":     ["Furniture", "Kitchen Appliances", "Garden Tools", "Lighting"],
    "Sports & Outdoors": ["Fitness Equipment", "Outdoor Gear", "Team Sports"],
    "Books & Media":     ["Programming", "Business", "Fiction", "Art Supplies"],
    "Food & Beverages":  ["Organic Food", "Beverages", "Snacks"],
    "Beauty & Health":   ["Skincare", "Supplements", "Hair Care"],
}

# ── New stores to add ─────────────────────────────────────────────────────────

EXTRA_STORE_NAMES = ["SportZone", "BookCorner", "HomeStyle"]

# ── Products per store ────────────────────────────────────────────────────────
# Each tuple: (product_name, price, stock_quantity, category_name)
# category_name must exist in CAT_IMAGE so the image resolves correctly.

STORE_PRODUCTS = {
    "SportZone": [
        ("Yoga Mat TPE 6mm",         599.99,  200, "Fitness Equipment"),
        ("Resistance Bands 5pcs",    299.99,  350, "Fitness Equipment"),
        ("Dumbbell Set 20kg",       2499.99,   40, "Fitness Equipment"),
        ("Pull-Up Bar Door",         799.99,   80, "Fitness Equipment"),
        ("Foam Roller 60cm",         399.99,  250, "Fitness Equipment"),
        ("Speed Jump Rope",          149.99,  400, "Fitness Equipment"),
        ("Kettlebell 16kg Cast",     999.99,   60, "Fitness Equipment"),
        ("Exercise Mat XL",          249.99,  300, "Fitness Equipment"),
        ("Trekking Backpack 40L",   1499.99,   70, "Outdoor Gear"),
        ("Trail Running Shoes",     1299.99,   90, "Outdoor Gear"),
        ("Cycling Helmet Pro",       999.99,   60, "Outdoor Gear"),
        ("Swimming Goggles UV",      199.99,  300, "Outdoor Gear"),
        ("Gym Duffel Bag 40L",       449.99,  200, "Outdoor Gear"),
        ("Water Bottle 1L BPA",      199.99,  500, "Outdoor Gear"),
        ("Tennis Racket Pro",       1799.99,   50, "Team Sports"),
        ("Football Official Size 5", 399.99,  200, "Team Sports"),
        ("Basketball Size 7",        299.99,  150, "Team Sports"),
        ("Boxing Gloves 12oz",       599.99,   80, "Team Sports"),
        ("Fitness Tracker Band",    1299.99,  100, "Fitness Equipment"),
        ("Protein Shaker 700ml",     149.99,  600, "Fitness Equipment"),
    ],
    "BookCorner": [
        ("Python for Data Science",     149.99, 200, "Programming"),
        ("Clean Code Book",             159.99, 300, "Programming"),
        ("System Design Interview",     189.99, 180, "Programming"),
        ("The Pragmatic Programmer",    169.99, 250, "Programming"),
        ("Machine Learning Book",       229.99, 120, "Programming"),
        ("Docker & Kubernetes Guide",   179.99, 150, "Programming"),
        ("AWS Certified Solutions",     199.99, 100, "Programming"),
        ("JavaScript The Complete",     149.99, 200, "Programming"),
        ("Atomic Habits TR",             69.99, 600, "Fiction"),
        ("The Alchemist TR",             59.99, 500, "Fiction"),
        ("Psychology of Money TR",       79.99, 400, "Business"),
        ("Rich Dad Poor Dad TR",         74.99, 350, "Business"),
        ("History of Turkey Vol1",       89.99, 200, "Fiction"),
        ("Economics 101 TR",             99.99, 250, "Business"),
        ("Watercolor Paint Kit 36",     349.99,  80, "Art Supplies"),
        ("Colored Pencils 72pc",        249.99, 150, "Art Supplies"),
        ("Sketch Notebook A4 120g",      79.99, 500, "Art Supplies"),
        ("Oil Paint Set Professional",  499.99,  60, "Art Supplies"),
        ("Manga Drawing Step Guide",     99.99, 180, "Art Supplies"),
        ("Photography Basics Book",     129.99, 120, "Programming"),
    ],
    "HomeStyle": [
        ("Ergonomic Office Chair",    4999.99,  30, "Furniture"),
        ("Standing Desk 140cm",       8999.99,  15, "Furniture"),
        ("Storage Ottoman Large",     1299.99,  45, "Furniture"),
        ("Bookshelf 5-Tier Steel",    1799.99,  30, "Furniture"),
        ("Bamboo Cutting Board Set",   399.99, 200, "Kitchen Appliances"),
        ("Cast Iron Skillet 26cm",     899.99,  80, "Kitchen Appliances"),
        ("Air Purifier HEPA H13",     3499.99,  40, "Kitchen Appliances"),
        ("Robot Vacuum Mop Pro",      7999.99,  25, "Kitchen Appliances"),
        ("Coffee Maker 12-Cup",       2499.99,  60, "Kitchen Appliances"),
        ("Blender Pro 1500W",         1999.99,  50, "Kitchen Appliances"),
        ("Kitchen Scale Digital",      299.99, 200, "Kitchen Appliances"),
        ("Plant Pot Set 3pcs",         349.99, 250, "Garden Tools"),
        ("Garden Hose 20m",            499.99, 100, "Garden Tools"),
        ("Pruning Shears Set",         249.99, 150, "Garden Tools"),
        ("Bed Sheets Set 400TC",       799.99, 150, "Furniture"),
        ("Blackout Curtains 2pcs",     649.99, 100, "Furniture"),
        ("Knit Throw Blanket",         399.99, 180, "Furniture"),
        ("Smart Thermostat WiFi",     2999.99,  35, "Lighting"),
        ("LED Desk Lamp Dimmable",     599.99, 120, "Lighting"),
        ("Picture Frames Set 6pcs",    249.99, 350, "Lighting"),
    ],
}

# ── Enum values ───────────────────────────────────────────────────────────────

# shipments.status smallint: 0=PENDING 1=LABEL_CREATED 2=IN_TRANSIT 3=OUT_FOR_DELIVERY 4=DELIVERED 5=RETURNED 6=CANCELLED
SHIP_STATUS_VALUES  = [0, 1, 2, 3, 4, 5, 6]
SHIP_STATUS_WEIGHTS = [0.05, 0.07, 0.15, 0.10, 0.53, 0.05, 0.05]
SHIP_MODES          = ["Standard", "Express", "Same Day"]
WAREHOUSES          = ["Istanbul-Main", "Ankara-DC", "Izmir-Hub", "Bursa-WH", "Antalya-DC"]

ORDER_STATUSES  = ["PENDING", "PAID", "SHIPPED", "PARTIALLY_SHIPPED", "DELIVERED", "CANCELLED"]
ORDER_WEIGHTS   = [0.05, 0.10, 0.15, 0.05, 0.55, 0.10]
PAY_METHODS     = ["CREDIT_CARD", "BANK_TRANSFER", "CASH_ON_DELIVERY"]
PAY_STATUSES    = ["SUCCESS", "PENDING", "FAILED"]
PAY_WEIGHTS     = [0.85, 0.10, 0.05]

# customer_profiles.membership_type smallint: 0=STANDARD 1=PREMIUM 2=VIP
MEM_VALUES  = [0, 1, 2]
MEM_WEIGHTS = [0.60, 0.30, 0.10]

CITIES = ["New York", "London", "Berlin", "Paris", "Toronto", "Sydney", "Amsterdam", "Madrid", "Dubai", "Singapore"]

REVIEW_COMMENTS = {
    5: ["Absolutely love it, highly recommend!", "Exceeded my expectations.", "Best purchase I've made.", "Excellent quality!", "Will definitely buy again."],
    4: ["Good product overall, minor issues.", "Great value for the price.", "Very satisfied with this.", "Solid purchase.", "Recommend to others."],
    3: ["Decent but could be better.", "Average product.", "Gets the job done.", "Nothing special.", "Somewhat met my expectations."],
    2: ["Disappointed with the quality.", "Not worth the price.", "Expected better.", "Would not recommend.", "Won't buy again."],
    1: ["Terrible, returned it immediately.", "Complete waste of money.", "Absolutely awful.", "Stay away from this!", "Worst purchase ever."],
}

# ── Seeders ───────────────────────────────────────────────────────────────────

def seed_extra_categories(cur):
    """Insert new categories, return name→id dict for all categories in DB."""
    print("  📂 Extra categories...")
    added = 0

    for parent_name, children in EXTRA_CATEGORIES.items():
        cur.execute(
            "INSERT INTO categories (name, created_at, updated_at) VALUES (%s, NOW(), NOW()) RETURNING id",
            (parent_name,)
        )
        parent_id = cur.fetchone()[0]
        added += 1
        for cname in children:
            cur.execute(
                "INSERT INTO categories (name, parent_id, created_at, updated_at) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
                (cname, parent_id)
            )
            cur.fetchone()
            added += 1

    # Build full name→id map from all categories now in DB
    cur.execute("SELECT id, name FROM categories")
    cat_name_to_id = {row[1]: row[0] for row in cur.fetchall()}

    print(f"      ✓ {added} new categories ({len(cat_name_to_id)} total in DB)")
    return cat_name_to_id


def seed_extra_corporate_users_and_stores(cur):
    print("  🏢 Extra corporate users & stores...")
    new_store_ids = []

    for sname in EXTRA_STORE_NAMES:
        email   = f"store.{sname.lower()}@platform.com"
        created = rdate(200, 400)
        cur.execute(
            "INSERT INTO users (email, password_hash, provider, role_type, is_active, created_at, updated_at) "
            "VALUES (%s, %s, 'LOCAL', 'CORPORATE', true, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
            (email, PASSWORD_HASH, created, created)
        )
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT id FROM users WHERE email=%s", (email,))
            row = cur.fetchone()
        uid = row[0]

        cur.execute(
            "INSERT INTO stores (name, status, owner_id, created_at, updated_at) "
            "VALUES (%s, 'ACTIVE', %s, %s, %s) ON CONFLICT (owner_id) DO NOTHING RETURNING id",
            (sname, uid, created, created)
        )
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT id FROM stores WHERE owner_id=%s", (uid,))
            row = cur.fetchone()
        if row:
            new_store_ids.append(row[0])

    cur.execute("SELECT id FROM stores")
    all_store_ids = [r[0] for r in cur.fetchall()]
    print(f"      ✓ {len(new_store_ids)} new stores added ({len(all_store_ids)} total)")
    return all_store_ids, new_store_ids


def seed_individual_users(cur):
    print("  👥 Individual users...")
    ind_ids = []

    for _ in range(NUM_EXTRA_INDIVIDUAL_USERS):
        email   = fake.email()
        created = rdate(0, 365)
        cur.execute(
            "INSERT INTO users (email, password_hash, provider, role_type, is_active, created_at, updated_at) "
            "VALUES (%s, %s, 'LOCAL', 'INDIVIDUAL', true, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
            (email, PASSWORD_HASH, created, created)
        )
        row = cur.fetchone()
        if row:
            ind_ids.append(row[0])

    cur.execute("SELECT id FROM users WHERE role_type='INDIVIDUAL'")
    all_ind = [r[0] for r in cur.fetchall()]
    print(f"      ✓ {len(ind_ids)} new users ({len(all_ind)} total individual)")
    return all_ind


def seed_profiles_and_addresses(cur, ind_ids):
    print("  📋 Profiles & addresses...")
    addr_by_user = {}

    for uid in ind_ids:
        city           = random.choice(CITIES)
        membership_int = wchoice(MEM_VALUES, MEM_WEIGHTS)

        cur.execute(
            "INSERT INTO customer_profiles (user_id, age, city, state, country, membership_type, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, 'Global', %s, NOW(), NOW()) ON CONFLICT (user_id) DO NOTHING",
            (uid, random.randint(18, 65), city, city, membership_int)
        )

        cur.execute("SELECT id FROM addresses WHERE user_id=%s", (uid,))
        existing = [r[0] for r in cur.fetchall()]
        addr_by_user[uid] = existing

        if not existing:
            for _ in range(random.randint(1, 2)):
                cur.execute(
                    "INSERT INTO addresses (user_id, city, district, full_address, phone_number, zip_code, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id",
                    (uid, city, fake.city(), fake.street_address()[:200],
                     fake.phone_number()[:20], fake.postcode()[:10])
                )
                addr_by_user[uid].append(cur.fetchone()[0])

    print(f"      ✓ done")
    return addr_by_user


def seed_carts(cur, ind_ids):
    for uid in ind_ids:
        cur.execute(
            "INSERT INTO carts (user_id, total_price, created_at, updated_at) "
            "VALUES (%s, 0.00, NOW(), NOW()) ON CONFLICT (user_id) DO NOTHING",
            (uid,)
        )


def seed_products(cur, new_store_ids, cat_name_to_id):
    """
    Insert products for new stores.
    Each product gets:
      - category_id  : looked up by category_name from STORE_PRODUCTS
      - image_url    : CAT_IMAGE[category_name]  (one image per category)
    Also loads existing products for use in orders/reviews.
    """
    print("  📦 Products...")
    all_products = []  # (product_id, Decimal(price), store_id)

    # Load existing products
    cur.execute("SELECT id, unit_price, store_id FROM products")
    for row in cur.fetchall():
        all_products.append((row[0], Decimal(str(row[1])), row[2]))

    # Build store_id → store_name map
    cur.execute("SELECT id, name FROM stores")
    store_name_map = {r[0]: r[1] for r in cur.fetchall()}

    new_count = 0
    for sid in new_store_ids:
        sname     = store_name_map.get(sid, "")
        templates = STORE_PRODUCTS.get(sname, [])
        if not templates:
            continue

        for j, (pname, price, stock, cat_name) in enumerate(templates):
            sku       = f"SKU-S{sid:03d}-{j+1:04d}"
            cat_id    = cat_name_to_id.get(cat_name)
            image_url = CAT_IMAGE.get(cat_name, DEFAULT_IMAGE)
            created   = rdate(100, 400)

            cur.execute(
                "INSERT INTO products (name, sku, unit_price, stock_quantity, store_id, category_id, "
                "description, image_url, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (sku) DO NOTHING RETURNING id",
                (pname, sku, price, stock, sid, cat_id,
                 f"Quality {pname.lower()} for everyday use.",
                 image_url, created, created)
            )
            row = cur.fetchone()
            if row:
                all_products.append((row[0], Decimal(str(price)), sid))
                new_count += 1

    print(f"      ✓ {new_count} new products ({len(all_products)} total)")
    return all_products


def seed_orders(cur, ind_ids, addr_by_user, all_products):
    print("  🛍️  Orders, payments, shipments...")
    users_with_addr = [u for u in ind_ids if addr_by_user.get(u)]
    order_cnt = 0

    for _ in range(NUM_ORDERS):
        if not users_with_addr or not all_products:
            break

        uid        = random.choice(users_with_addr)
        addr_id    = random.choice(addr_by_user[uid])
        status     = wchoice(ORDER_STATUSES, ORDER_WEIGHTS)
        order_date = rdate(0, 365)

        picked   = random.sample(all_products, min(random.randint(1, 4), len(all_products)))
        by_store = {}
        for pid, price, sid in picked:
            by_store.setdefault(sid, []).append((pid, price))

        if len(by_store) == 1:
            sid, items = list(by_store.items())[0]
            grand  = sum(p * random.randint(1, 3) for _, p in items)
            ship_c = Decimal("29.99") if grand < 500 else Decimal("0.00")
            total  = grand + ship_c

            cur.execute(
                "INSERT INTO orders (status, grand_total, order_date, shipping_cost, "
                "user_id, store_id, address_id, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (status, total, order_date, ship_c, uid, sid, addr_id, order_date, order_date)
            )
            oid = cur.fetchone()[0]
            for pid, price in items:
                cur.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, price, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (oid, pid, random.randint(1, 3), price, order_date, order_date)
                )
            _insert_payment(cur, oid, total, order_date)
            if status not in ("PENDING", "CANCELLED"):
                _insert_shipment(cur, oid, order_date)

        else:
            parent_total = sum(p for _, p, _ in picked)
            cur.execute(
                "INSERT INTO orders (status, grand_total, order_date, shipping_cost, "
                "user_id, address_id, created_at, updated_at) "
                "VALUES (%s, %s, %s, 0.00, %s, %s, %s, %s) RETURNING id",
                (status, parent_total, order_date, uid, addr_id, order_date, order_date)
            )
            parent_oid = cur.fetchone()[0]
            _insert_payment(cur, parent_oid, parent_total, order_date)

            for sid, items in by_store.items():
                child_total = sum(p * random.randint(1, 2) for _, p in items)
                ship_c      = Decimal("29.99") if child_total < 500 else Decimal("0.00")
                cur.execute(
                    "INSERT INTO orders (status, grand_total, order_date, shipping_cost, "
                    "user_id, store_id, address_id, parent_order_id, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (status, child_total + ship_c, order_date, ship_c,
                     uid, sid, addr_id, parent_oid, order_date, order_date)
                )
                coid = cur.fetchone()[0]
                for pid, price in items:
                    cur.execute(
                        "INSERT INTO order_items (order_id, product_id, quantity, price, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (coid, pid, random.randint(1, 2), price, order_date, order_date)
                    )
                if status not in ("PENDING", "CANCELLED"):
                    _insert_shipment(cur, coid, order_date)

        order_cnt += 1

    print(f"      ✓ {order_cnt} orders (+ sub-orders, payments, shipments)")


def _insert_payment(cur, order_id, amount, order_date):
    pay_status = wchoice(PAY_STATUSES, PAY_WEIGHTS)
    cur.execute(
        "INSERT INTO payments (order_id, amount, payment_method, status, transaction_key, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING",
        (order_id, amount, random.choice(PAY_METHODS), pay_status,
         str(uuid.uuid4()), order_date, order_date)
    )


def _insert_shipment(cur, order_id, order_date):
    ship_status_int = wchoice(SHIP_STATUS_VALUES, SHIP_STATUS_WEIGHTS)
    est_delivery    = order_date + timedelta(days=random.randint(1, 7))
    cur.execute(
        "INSERT INTO shipments (order_id, warehouse, mode, status, tracking_number, "
        "estimated_delivery_date, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (order_id, random.choice(WAREHOUSES), random.choice(SHIP_MODES), ship_status_int,
         f"TR{random.randint(100000000, 999999999)}",
         est_delivery, order_date, order_date)
    )


def seed_reviews(cur, ind_ids, all_products):
    print("  ⭐ Reviews...")
    cur.execute("SELECT user_id, product_id FROM reviews")
    seen  = set(tuple(r) for r in cur.fetchall())
    count = 0

    for _ in range(NUM_REVIEWS):
        uid       = random.choice(ind_ids)
        pid, _, _ = random.choice(all_products)
        if (uid, pid) in seen:
            continue
        seen.add((uid, pid))

        star      = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.17, 0.35, 0.35])[0]
        sentiment = "POSITIVE" if star >= 4 else ("NEGATIVE" if star <= 2 else "NEUTRAL")
        created   = rdate(0, 300)

        cur.execute(
            "INSERT INTO reviews (product_id, user_id, star_rating, sentiment, comment_text, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (pid, uid, star, sentiment, random.choice(REVIEW_COMMENTS[star]), created, created)
        )
        count += 1

    print(f"      ✓ {count} reviews")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n🌱 Starting database seed (additive — existing data preserved)...\n")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        cat_name_to_id = seed_extra_categories(cur)
        conn.commit()

        all_store_ids, new_store_ids = seed_extra_corporate_users_and_stores(cur)
        conn.commit()

        ind_ids = seed_individual_users(cur)
        conn.commit()

        addr_by_user = seed_profiles_and_addresses(cur, ind_ids)
        conn.commit()

        seed_carts(cur, ind_ids)
        conn.commit()

        all_products = seed_products(cur, new_store_ids, cat_name_to_id)
        conn.commit()

        seed_orders(cur, ind_ids, addr_by_user, all_products)
        conn.commit()

        seed_reviews(cur, ind_ids, all_products)
        conn.commit()

        print("\n✅ Seed complete! All data added on top of existing records.")

    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        print(f"\n❌ Seed failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
