import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", 5432),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

def list_keywords():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT keyword, active, added_at FROM tracked_keywords ORDER BY added_at")
    rows = cur.fetchall()
    print("\nCurrent tracked keywords:")
    print("─" * 50)
    for row in rows:
        status = "✅ active" if row[1] else "❌ paused"
        print(f"  {row[0]:<30} {status}")
    print()
    cur.close()
    conn.close()

def add_keyword(keyword: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tracked_keywords (keyword)
        VALUES (%s)
        ON CONFLICT (keyword) DO UPDATE SET active = TRUE
    """, (keyword,))
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Added: '{keyword}'")

def remove_keyword(keyword: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        UPDATE tracked_keywords
        SET active = FALSE
        WHERE keyword = %s
    """, (keyword,))
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Deactivated: '{keyword}'")

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        list_keywords()
    elif sys.argv[1] == "add" and len(sys.argv) == 3:
        add_keyword(sys.argv[2])
        list_keywords()
    elif sys.argv[1] == "remove" and len(sys.argv) == 3:
        remove_keyword(sys.argv[2])
        list_keywords()
    else:
        print("Usage:")
        print("  python3 manage_keywords.py")
        print("  python3 manage_keywords.py add 'influencer marketing'")
        print("  python3 manage_keywords.py remove 'SEO'")