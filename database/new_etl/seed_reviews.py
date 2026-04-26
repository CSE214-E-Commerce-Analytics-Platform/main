#!/usr/bin/env python3
"""
Review Seeder — adds balanced reviews to the 30 new stores' products (EXT-* SKUs).

Safe to re-run: skips (user_id, product_id) pairs that already exist.

Run AFTER seed_extra.py:
    python seed_reviews.py
"""

import os
import random
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)
random.seed(42)

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

def rdate(days_ago_min=1, days_ago_max=300):
    delta = random.randint(days_ago_min, days_ago_max)
    return datetime.now() - timedelta(days=delta, seconds=random.randint(0, 86400))

# ── Review templates per star level ───────────────────────────────────────────

REVIEWS = {
    1: [
        "Very disappointed. Product did not match the description at all.",
        "Terrible quality, broke within a day. Would not recommend.",
        "Arrived damaged and customer support was unhelpful.",
        "Complete waste of money. Nothing like the photos.",
        "Worst purchase I've made. Returning immediately.",
        "Does not work as advertised. Very poor build quality.",
    ],
    2: [
        "Below average quality for the price. Expected better.",
        "Packaging was decent but the product itself is underwhelming.",
        "Works, but barely. Had to make adjustments right away.",
        "Not happy with this purchase. Several features are missing.",
        "Quality feels cheap. I've seen better at lower prices.",
        "Instructions were unclear and setup was frustrating.",
    ],
    3: [
        "Average product. Does the job but nothing special.",
        "Okay for the price. A few minor issues but usable.",
        "Decent enough. Would consider buying again if improved.",
        "Neutral experience. Some pros, some cons.",
        "Product is fine, delivery was slow though.",
        "Not bad, not great. Meets basic expectations.",
        "It works as described but I expected a bit more.",
    ],
    4: [
        "Good product overall. Happy with my purchase.",
        "Works well and looks great. Minor packaging issue on arrival.",
        "Solid quality for the price. Would recommend.",
        "Very satisfied. Fast shipping and good build quality.",
        "Great value. Exactly what I needed.",
        "Product performs as expected. Good customer experience.",
        "Happy with this. Would buy from this store again.",
    ],
    5: [
        "Absolutely love it! Exceeded all my expectations.",
        "Perfect product. Arrived quickly and works flawlessly.",
        "Outstanding quality. Best purchase this month!",
        "Highly recommend. Fantastic product and great packaging.",
        "Five stars without hesitation. Exactly as described.",
        "Superb! Will definitely be ordering more from this store.",
        "Top-notch quality. Very impressed with the whole experience.",
        "Excellent product at a fair price. Delivery was super fast.",
    ],
}

# Realistic star distribution (slightly positive, not Amazon-biased)
# 1★ 8%  2★ 10%  3★ 20%  4★ 35%  5★ 27%
STAR_WEIGHTS = [8, 10, 20, 35, 27]
STARS = [1, 2, 3, 4, 5]

def sentiment_for(star: int) -> str:
    if star >= 4:
        return "POSITIVE"
    if star <= 2:
        return "NEGATIVE"
    return "NEUTRAL"

# ── Main seeder ───────────────────────────────────────────────────────────────

def seed_reviews(conn, cur):
    print("  ⭐ Seeding reviews for new-store products (EXT-* SKUs)...")

    # Target: products belonging to the 30 new stores (identified by EXT- SKU prefix)
    cur.execute("SELECT id FROM products WHERE sku LIKE 'EXT-%'")
    product_ids = [r[0] for r in cur.fetchall()]

    if not product_ids:
        print("      ⚠️  No EXT-* products found. Run seed_extra.py first.")
        return

    print(f"      Found {len(product_ids)} products to review.")

    # Individual users as reviewers
    cur.execute("SELECT id FROM users WHERE role_type = 'INDIVIDUAL' LIMIT 5000")
    user_ids = [r[0] for r in cur.fetchall()]

    if not user_ids:
        print("      ⚠️  No individual users found.")
        return

    # Load existing (user_id, product_id) pairs to avoid duplicates
    cur.execute("SELECT user_id, product_id FROM reviews")
    existing = set(tuple(r) for r in cur.fetchall())

    added = 0
    skipped = 0

    for product_id in product_ids:
        # Each product gets 3-6 reviews
        n_reviews = random.randint(3, 6)
        reviewers = random.sample(user_ids, min(n_reviews, len(user_ids)))

        for uid in reviewers:
            if (uid, product_id) in existing:
                skipped += 1
                continue

            existing.add((uid, product_id))
            star = random.choices(STARS, weights=STAR_WEIGHTS, k=1)[0]
            sentiment = sentiment_for(star)
            comment = random.choice(REVIEWS[star])
            rev_date = rdate(1, 270)

            cur.execute(
                "INSERT INTO reviews (product_id, user_id, star_rating, sentiment, comment_text, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (product_id, uid, star, sentiment, comment, rev_date, rev_date)
            )
            added += 1

        if added % 500 == 0 and added > 0:
            conn.commit()

    conn.commit()
    print(f"      ✓ {added} reviews added ({skipped} skipped — already existed)")

    # Print distribution summary
    cur.execute("""
        SELECT r.sentiment, COUNT(*)
        FROM reviews r
        JOIN products p ON r.product_id = p.id
        WHERE p.sku LIKE 'EXT-%'
        GROUP BY r.sentiment
        ORDER BY r.sentiment
    """)
    rows = cur.fetchall()
    print("\n      Sentiment distribution for new-store products:")
    for sentiment, count in rows:
        print(f"        {sentiment}: {count}")

    cur.execute("""
        SELECT r.star_rating, COUNT(*)
        FROM reviews r
        JOIN products p ON r.product_id = p.id
        WHERE p.sku LIKE 'EXT-%'
        GROUP BY r.star_rating
        ORDER BY r.star_rating
    """)
    rows = cur.fetchall()
    print("\n      Star rating distribution:")
    for star, count in rows:
        print(f"        {star}★: {count}")


def main():
    print("\n⭐ Starting review seeder for new stores...\n")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        seed_reviews(conn, cur)
        print("\n✅ Review seeding complete!\n")

    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        print(f"\n❌ Failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
