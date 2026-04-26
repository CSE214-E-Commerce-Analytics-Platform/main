#!/usr/bin/env python3
"""
Audit Log Seeder — populates audit_logs with realistic admin/corporate actions.

Safe to run at any time: audit_logs has no unique constraint, so each run
appends new entries. Run once after seed_extra.py for best results.

Run:
    python seed_audit_logs.py
"""

import os
import random
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)
random.seed(55)

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

def rdate(days_ago_min=1, days_ago_max=400):
    delta = random.randint(days_ago_min, days_ago_max)
    return datetime.now() - timedelta(days=delta, seconds=random.randint(0, 86400))

# ── Action templates ──────────────────────────────────────────────────────────

ADMIN_ACTIONS = [
    ("USER_UPDATED",               "Updated user id={uid} — role or status changed."),
    ("USER_DEACTIVATED",           "Deactivated user id={uid} due to policy violation."),
    ("USER_ACTIVATED",             "Re-activated user id={uid} after appeal approved."),
    ("CORPORATE_REQUEST_REVIEWED", "Corporate request id={rid} → APPROVED | applicant={email}"),
    ("CORPORATE_REQUEST_REJECTED", "Corporate request id={rid} → REJECTED | reason: insufficient documentation."),
    ("USER_DELETED",               "Deleted user id={uid} — account removal requested."),
    ("STORE_STATUS_UPDATED",       "Store id={sid} status set to ACTIVE."),
    ("PRODUCT_REMOVED",            "Removed product id={pid} — policy violation."),
    ("PASSWORD_RESET_FORCED",      "Forced password reset for user id={uid}."),
    ("BULK_USER_REVIEW",           "Reviewed {n} inactive accounts — {n2} deactivated."),
]

CORPORATE_ACTIONS = [
    ("PRODUCT_CREATED",   "Created product id={pid} in store id={sid}."),
    ("PRODUCT_UPDATED",   "Updated product id={pid} — price or stock changed."),
    ("SHIPMENT_CREATED",  "Initiated shipment for order id={oid}, tracking={trk}."),
    ("STORE_UPDATED",     "Updated store id={sid} — name or description changed."),
    ("ORDER_ACCEPTED",    "Accepted order id={oid} for processing."),
]

INDIVIDUAL_ACTIONS = [
    ("USER_LOGIN",          "User id={uid} logged in from web."),
    ("PROFILE_UPDATED",     "User id={uid} updated profile information."),
    ("ADDRESS_ADDED",       "User id={uid} added a new delivery address."),
    ("PASSWORD_CHANGED",    "User id={uid} changed their password."),
    ("ORDER_PLACED",        "User id={uid} placed order id={oid}."),
]

# ── Seed ──────────────────────────────────────────────────────────────────────

def seed_audit_logs(conn, cur):
    print("  📋 Seeding audit logs...")

    # Fetch user IDs per role
    cur.execute("SELECT id FROM users WHERE role_type = 'ADMIN'")
    admin_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM users WHERE role_type = 'CORPORATE' LIMIT 200")
    corp_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM users WHERE role_type = 'INDIVIDUAL' LIMIT 500")
    ind_ids = [r[0] for r in cur.fetchall()]

    if not admin_ids:
        print("      ⚠️  No admin users found.")
        return

    # Fetch some product, order, store IDs for realistic detail strings
    cur.execute("SELECT id FROM products ORDER BY RANDOM() LIMIT 200")
    product_ids = [r[0] for r in cur.fetchall()] or [1]

    cur.execute("SELECT id FROM orders WHERE store_id IS NOT NULL ORDER BY RANDOM() LIMIT 200")
    order_ids = [r[0] for r in cur.fetchall()] or [1]

    cur.execute("SELECT id FROM stores ORDER BY RANDOM() LIMIT 50")
    store_ids = [r[0] for r in cur.fetchall()] or [1]

    cur.execute("SELECT id FROM corporate_update_requests ORDER BY RANDOM() LIMIT 50")
    req_ids = [r[0] for r in cur.fetchall()] or [1]

    cur.execute("SELECT email FROM users ORDER BY RANDOM() LIMIT 100")
    emails = [r[0] for r in cur.fetchall()] or ["user@demo.com"]

    def fmt(template, uid=None, pid=None, oid=None, sid=None, rid=None):
        return template.format(
            uid=uid or random.choice(ind_ids or admin_ids),
            pid=pid or random.choice(product_ids),
            oid=oid or random.choice(order_ids),
            sid=sid or random.choice(store_ids),
            rid=rid or random.choice(req_ids),
            email=random.choice(emails),
            trk="FRK-" + format(random.randint(0, 0xFFFFFF), "06X"),
            n=random.randint(10, 50),
            n2=random.randint(1, 5),
        )

    def insert_log(user_id, user_role, action, details, created_at):
        cur.execute(
            "INSERT INTO audit_logs (user_id, user_role, action, details, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (user_id, user_role, action, details, created_at, created_at)
        )

    added = 0

    # ── Admin logs: ~120 entries ──────────────────────────────────────────────
    for _ in range(120):
        admin_id = random.choice(admin_ids)
        action, detail_tmpl = random.choice(ADMIN_ACTIONS)
        details = fmt(detail_tmpl, uid=random.choice((ind_ids or corp_ids or [admin_id])))
        insert_log(admin_id, "ADMIN", action, details, rdate(1, 400))
        added += 1

    # ── Corporate logs: ~80 entries ───────────────────────────────────────────
    if corp_ids:
        for _ in range(80):
            corp_id = random.choice(corp_ids)
            action, detail_tmpl = random.choice(CORPORATE_ACTIONS)
            details = fmt(detail_tmpl, uid=corp_id, sid=random.choice(store_ids))
            insert_log(corp_id, "CORPORATE", action, details, rdate(1, 300))
            added += 1

    # ── Individual logs: ~100 entries ─────────────────────────────────────────
    if ind_ids:
        for _ in range(100):
            ind_id = random.choice(ind_ids)
            action, detail_tmpl = random.choice(INDIVIDUAL_ACTIONS)
            details = fmt(detail_tmpl, uid=ind_id)
            insert_log(ind_id, "INDIVIDUAL", action, details, rdate(1, 200))
            added += 1

    conn.commit()
    print(f"      ✓ {added} audit log entries added")

    # Summary
    cur.execute("SELECT user_role, COUNT(*) FROM audit_logs GROUP BY user_role ORDER BY user_role")
    rows = cur.fetchall()
    print("\n      Audit log breakdown (total DB):")
    for role, count in rows:
        print(f"        {role}: {count}")


def main():
    print("\n📋 Starting audit log seeder...\n")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        seed_audit_logs(conn, cur)
        print("\n✅ Audit log seeding complete!\n")

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
