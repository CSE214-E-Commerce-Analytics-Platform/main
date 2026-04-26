#!/usr/bin/env python3
"""
E-Commerce Analytics Platform — Extra Seeder
Adds:
  - 30 new CORPORATE users + stores (distinct from existing 5)
  - APPROVED CorporateUpdateRequest for each new corporate
  - 8 PENDING CorporateUpdateRequest records from existing individual users
  - Existing products redistributed to new stores (no new products created from scratch)

Does NOT:
  - Add new individual users
  - Add new products from scratch
  - Modify existing 5 corporate stores or their data

Run AFTER seed.py and etl_csv.py:
    python seed_extra.py
"""

import os
import random
from datetime import datetime, timedelta

import bcrypt
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)
random.seed(77)

PASSWORD_HASH = bcrypt.hashpw(b"password123", bcrypt.gensalt(10)).decode()

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
    conn.set_client_encoding("UTF8")
    return conn

def rdate(days_ago_min=1, days_ago_max=500):
    delta = random.randint(days_ago_min, days_ago_max)
    return datetime.now() - timedelta(days=delta, seconds=random.randint(0, 86400))

# ── 30 new corporate store definitions ────────────────────────────────────────
# focus_keywords: matched against category names to assign relevant existing products

NEW_CORP_STORES = [
    {"email": "corp.techhub@platform.com",     "company": "TechHub Electronics Ltd.",   "store": "TechHub Electronics",  "focus": ["electronics", "computer", "laptop", "phone"],           "reason": "5 years of retail electronics experience with nationwide distribution."},
    {"email": "corp.mobileworld@platform.com",  "company": "MobileWorld Inc.",           "store": "MobileWorld",          "focus": ["smartphone", "mobile", "phone", "accessori"],           "reason": "Authorized reseller for major smartphone brands across the country."},
    {"email": "corp.booknest@platform.com",     "company": "BookNest Publishing House",  "store": "BookNest",             "focus": ["book", "novel", "media", "guide", "manual"],            "reason": "Independent book retailer with over 10,000 titles in our catalog."},
    {"email": "corp.fitlife@platform.com",      "company": "FitLife Sports Co.",         "store": "FitLife Sports",       "focus": ["sport", "fitness", "gym", "yoga", "exercise"],          "reason": "Premium fitness equipment supplier for gyms and individual athletes."},
    {"email": "corp.kitchenpro@platform.com",   "company": "KitchenPro Appliances",      "store": "KitchenPro",           "focus": ["kitchen", "cook", "appliance", "beverage", "food"],     "reason": "Specialized kitchen appliance retailer with 12 years of experience."},
    {"email": "corp.stylezone@platform.com",    "company": "StyleZone Fashion Group",    "store": "StyleZone Fashion",    "focus": ["clothing", "apparel", "fashion", "shirt", "dress"],     "reason": "Fast-fashion retailer delivering seasonal collections nationwide."},
    {"email": "corp.greengarden@platform.com",  "company": "GreenGarden Ltd.",           "store": "GreenGarden",          "focus": ["garden", "plant", "outdoor", "tool"],                   "reason": "Garden and outdoor lifestyle products specialist since 2012."},
    {"email": "corp.lamplight@platform.com",    "company": "LampLight Illumination",     "store": "LampLight",            "focus": ["light", "lamp", "led", "bulb", "lighting"],             "reason": "Energy-efficient lighting solutions for homes and commercial spaces."},
    {"email": "corp.vitashop@platform.com",     "company": "VitaShop Health",            "store": "VitaShop",             "focus": ["supplement", "vitamin", "health", "protein"],           "reason": "Certified health supplement retailer with lab-tested products."},
    {"email": "corp.homefurni@platform.com",    "company": "HomeFurni Interiors",        "store": "HomeFurni",            "focus": ["furniture", "chair", "desk", "sofa", "table"],          "reason": "Modern and affordable home furniture for every style and budget."},
    {"email": "corp.camerazone@platform.com",   "company": "CameraZone Pro",             "store": "CameraZone",           "focus": ["camera", "photo", "lens", "gopro"],                     "reason": "Professional photography equipment and accessories dealer."},
    {"email": "corp.skincareplus@platform.com", "company": "SkincareGlow Ltd.",          "store": "Skincare Plus",        "focus": ["beauty", "skin", "care", "cosmetic", "makeup"],         "reason": "Dermatologist-recommended skincare and beauty products."},
    {"email": "corp.gamingarena@platform.com",  "company": "GamingArena Digital",        "store": "GamingArena",          "focus": ["gaming", "keyboard", "mouse", "headphone", "game"],     "reason": "Gaming peripherals and accessories for professional and casual gamers."},
    {"email": "corp.audiopeak@platform.com",    "company": "AudioPeak Sound",            "store": "AudioPeak",            "focus": ["audio", "speaker", "headphone", "sound", "earphone"],   "reason": "Hi-fi audio equipment and accessories for audiophiles."},
    {"email": "corp.smartoffice@platform.com",  "company": "SmartOffice Solutions",      "store": "SmartOffice",          "focus": ["cable", "usb", "charger", "peripheral", "accessories"],  "reason": "Office technology solutions and connectivity accessories provider."},
    {"email": "corp.tvhub@platform.com",        "company": "TVHub Display Tech",         "store": "TVHub",                "focus": ["television", "tv", "monitor", "display", "screen"],     "reason": "Display technology retailer with professional installation services."},
    {"email": "corp.shoestreet@platform.com",   "company": "ShoeStreet Footwear",        "store": "ShoeStreet",           "focus": ["shoe", "boot", "sneaker", "footwear"],                  "reason": "Premium and sports footwear with 200+ international brands."},
    {"email": "corp.tabletzone@platform.com",   "company": "TabletZone Digital",         "store": "TabletZone",           "focus": ["tablet", "ipad", "digital", "electronics"],             "reason": "Tablet and digital reading device specialist with expert support."},
    {"email": "corp.cyclefit@platform.com",     "company": "CycleFit Sports",            "store": "CycleFit",             "focus": ["sport", "fitness", "exercise", "gym", "dumbbell"],      "reason": "Cycling and outdoor sports equipment supplier for enthusiasts."},
    {"email": "corp.watchworld@platform.com",   "company": "WatchWorld Timepieces",      "store": "WatchWorld",           "focus": ["electronics", "smart", "watch", "accessories"],         "reason": "Authorized retailer for luxury and smart watch collections."},
    {"email": "corp.ecolife@platform.com",      "company": "EcoLife Sustainability",     "store": "EcoLife",              "focus": ["garden", "outdoor", "health", "supplement", "vitamin"], "reason": "Eco-friendly products and sustainable living essentials."},
    {"email": "corp.homedecor@platform.com",    "company": "HomeDecor Express",          "store": "HomeDecor Express",    "focus": ["furniture", "lamp", "light", "home", "decor"],          "reason": "Contemporary home decor and interior accessories for modern living."},
    {"email": "corp.sportpeak@platform.com",    "company": "SportPeak Athletic Co.",     "store": "SportPeak",            "focus": ["sport", "fitness", "yoga", "exercise", "athletic"],     "reason": "Professional athletic gear and training equipment supplier."},
    {"email": "corp.digitalbay@platform.com",   "company": "DigitalBay Online",          "store": "DigitalBay",           "focus": ["electronics", "laptop", "computer", "tablet", "phone"], "reason": "Online-first electronics retailer with competitive pricing."},
    {"email": "corp.fashionfit@platform.com",   "company": "FashionFit Active",          "store": "FashionFit",           "focus": ["clothing", "apparel", "sport", "fashion", "active"],    "reason": "Active lifestyle fashion brand with sports-casual collections."},
    {"email": "corp.beautypro@platform.com",    "company": "BeautyPro Cosmetics",        "store": "BeautyPro",            "focus": ["beauty", "skin", "cosmetic", "makeup", "care"],         "reason": "Professional beauty and cosmetics distributor with global brands."},
    {"email": "corp.nutrizone@platform.com",    "company": "NutriZone Wellness",         "store": "NutriZone",            "focus": ["supplement", "vitamin", "protein", "health", "nutrition"],"reason": "Sports nutrition and wellness supplement retailer with certified products."},
    {"email": "corp.gadgetking@platform.com",   "company": "GadgetKing Tech",            "store": "GadgetKing",           "focus": ["electronics", "usb", "cable", "charger", "gadget"],     "reason": "Gadgets, accessories, and tech novelties for everyday life."},
    {"email": "corp.mediahub@platform.com",     "company": "MediaHub Entertainment",     "store": "MediaHub",             "focus": ["book", "media", "audio", "speaker", "entertainment"],   "reason": "Books, media and entertainment products for all ages."},
    {"email": "corp.petcorner@platform.com",    "company": "PetCorner Supplies",         "store": "PetCorner",            "focus": ["general", "accessories", "health"],                     "reason": "Complete pet care store serving thousands of pet owners."},
]

# ── Admin users ───────────────────────────────────────────────────────────────

ADMIN_USERS = [
    {"email": "admin.sarah@platform.com",  "name": "Sarah Mitchell"},
    {"email": "admin.james@platform.com",  "name": "James Carter"},
    {"email": "admin.leyla@platform.com",  "name": "Leyla Yılmaz"},
]

# ── Pending request company templates (for individual users) ──────────────────

PENDING_COMPANIES = [
    ("NextGen Retail Ltd.",    "We plan to expand our offline retail presence online."),
    ("GrowFast Commerce",      "Seeking to reach new customer segments through e-commerce."),
    ("BlueStar Trading",       "Established offline retailer transitioning to digital."),
    ("SilverLine Goods",       "3 years of import/export experience ready for online sales."),
    ("UrbanCart Inc.",         "Urban lifestyle products targeting millennial shoppers."),
    ("PureBrand Co.",          "Organic and natural products brand seeking online reach."),
    ("FlexTrade Solutions",    "B2B and B2C wholesale solutions expanding to direct-to-consumer."),
    ("NovaBiz Group",          "Tech-forward startup entering e-commerce for the first time."),
]

# ── Seed functions ─────────────────────────────────────────────────────────────

def seed_admins(conn, cur):
    print("  👑 Adding 3 admin users...")
    added = 0
    for a in ADMIN_USERS:
        created = rdate(200, 600)
        cur.execute(
            "INSERT INTO users (email, password_hash, provider, role_type, is_active, created_at, updated_at) "
            "VALUES (%s, %s, 'LOCAL', 'ADMIN', true, %s, %s) ON CONFLICT (email) DO NOTHING",
            (a["email"], PASSWORD_HASH, created, created)
        )
        if cur.rowcount:
            added += 1
    conn.commit()
    print(f"      ✓ {added} admin users added")


def seed_corporates(conn, cur):
    print("  🏢 Adding 30 new corporate users + stores...")

    new_store_entries = []

    for i, s in enumerate(NEW_CORP_STORES):
        created = rdate(100, 400)

        # Insert corporate user
        cur.execute(
            "INSERT INTO users (email, password_hash, provider, role_type, is_active, created_at, updated_at) "
            "VALUES (%s, %s, 'LOCAL', 'CORPORATE', true, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
            (s["email"], PASSWORD_HASH, created, created)
        )
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT id FROM users WHERE email = %s", (s["email"],))
            row = cur.fetchone()
        if not row:
            continue

        uid = row[0]

        # Insert store (owner_id is UNIQUE — one store per corporate user)
        cur.execute(
            "SELECT id FROM stores WHERE owner_id = %s LIMIT 1", (uid,)
        )
        existing_store = cur.fetchone()
        if existing_store:
            store_id = existing_store[0]
        else:
            cur.execute(
                "INSERT INTO stores (name, owner_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'ACTIVE', %s, %s) RETURNING id",
                (s["store"][:255], uid, created, created)
            )
            store_row = cur.fetchone()
            if not store_row:
                continue
            store_id = store_row[0]

        new_store_entries.append((store_id, s))

        # APPROVED CorporateUpdateRequest
        # Entity fields: company_name, reason, status, admin_note, user_id
        cur.execute(
            "SELECT id FROM corporate_update_requests WHERE user_id = %s LIMIT 1", (uid,)
        )
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO corporate_update_requests "
                "(company_name, reason, status, admin_note, user_id, created_at, updated_at) "
                "VALUES (%s, %s, 'APPROVED', %s, %s, %s, %s)",
                (s["company"][:255], s["reason"][:2000],
                 "Application approved by admin.",
                 uid, created, created)
            )

        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"      ... {i + 1} corporates processed")

    conn.commit()
    print(f"      ✓ {len(new_store_entries)} new stores created")
    return new_store_entries


def redistribute_products(conn, cur, new_store_entries):
    """Copy existing products (with new SKUs) to new stores based on category keyword matching."""
    print("  📦 Redistributing existing products to new stores...")

    # Fetch all existing products with category names
    cur.execute("""
        SELECT p.name, p.unit_price, p.stock_quantity, p.category_id, p.description, p.image_url,
               COALESCE(c.name, '') AS category_name
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
    """)
    all_products = cur.fetchall()
    # cols: name, unit_price, stock_quantity, category_id, description, image_url, category_name

    if not all_products:
        print("      ⚠️  No existing products found — skipping redistribution")
        return

    total_added = 0

    for store_id, store_info in new_store_entries:
        focus = [kw.lower() for kw in store_info["focus"]]

        # Score products: count how many focus keywords appear in the category name
        scored = []
        for prod in all_products:
            cat_name = prod[6].lower()
            score = sum(1 for kw in focus if kw in cat_name)
            scored.append((score, prod))

        scored.sort(key=lambda x: -x[0])

        # Take top 80 by score, then randomly sample 20 from those
        pool = [p for _, p in scored[:80]] or [p for _, p in scored]
        selected = random.sample(pool, min(20, len(pool)))

        # Short tag for SKU generation (max 8 chars from store name)
        tag = store_info["store"].replace(" ", "")[:8].upper()

        for j, prod in enumerate(selected):
            name, price, stock, cat_id, desc, image_url, _ = prod
            new_sku = f"EXT-{tag}-{store_id}-{j + 1:03d}"

            cur.execute(
                "INSERT INTO products (name, sku, unit_price, stock_quantity, store_id, category_id, "
                "description, image_url, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) ON CONFLICT (sku) DO NOTHING",
                (name[:240], new_sku, price, random.randint(5, 200),
                 store_id, cat_id, (desc or "")[:250], image_url)
            )
            total_added += 1

        conn.commit()

    print(f"      ✓ {total_added} product listings distributed across {len(new_store_entries)} new stores")


def add_pending_requests(conn, cur, n=8):
    """Add PENDING corporate_update_requests from existing individual users who have none yet."""
    print("  📝 Adding pending corporate update requests from individual users...")

    cur.execute(
        "SELECT u.id FROM users u "
        "LEFT JOIN corporate_update_requests r ON r.user_id = u.id "
        "WHERE u.role_type = 'INDIVIDUAL' AND r.id IS NULL "
        "ORDER BY u.id "
        "LIMIT %s",
        (n * 4,)
    )
    candidates = [r[0] for r in cur.fetchall()]

    if not candidates:
        print("      ⚠️  No eligible individual users found")
        return

    selected = random.sample(candidates, min(n, len(candidates)))
    added = 0

    for uid in selected:
        company, reason = random.choice(PENDING_COMPANIES)
        created = rdate(1, 60)
        cur.execute(
            "INSERT INTO corporate_update_requests "
            "(company_name, reason, status, admin_note, user_id, created_at, updated_at) "
            "VALUES (%s, %s, 'PENDING', NULL, %s, %s, %s)",
            (company[:255], reason[:2000], uid, created, created)
        )
        added += 1

    conn.commit()
    print(f"      ✓ {added} pending requests added")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n🌱 Starting Extra Seed (additive — existing data untouched)...\n")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        seed_admins(conn, cur)
        new_store_entries = seed_corporates(conn, cur)
        redistribute_products(conn, cur, new_store_entries)
        add_pending_requests(conn, cur)

        print("\n✅ Extra seed complete!")
        print(f"   3 admins + 30 corporate users/stores added, products distributed, pending requests created.\n")

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
