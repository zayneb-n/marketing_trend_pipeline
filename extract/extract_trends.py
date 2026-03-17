import os
import json
import psycopg2
from datetime import datetime
from pytrends.request import TrendReq
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Config 
KEYWORDS = os.getenv("TREND_KEYWORDS", "AI marketing").split(",")
KEYWORDS = [k.strip() for k in KEYWORDS]

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", 5432),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

RAW_DIR = os.getenv("RAW_DIR", "./data/raw/trends")


# 1: Pull data from Google Trends unofficial API and return list of dicts with date and interest
def fetch_trends(keyword: str) -> list[dict]:
    print(f"  Fetching trends for: '{keyword}'")
    
    pytrends = TrendReq(
        hl="en-US",
        tz=0,
        timeout=(10, 25),        # connect timeout, read timeout
        retries=3,               # retry 3 times
        backoff_factor=0.5       # wait between retries
    ) 
    # Wait before hitting Google (mimics human behavior)
    time.sleep(5)
    
    pytrends.build_payload([keyword], timeframe="today 3-m")
    
    # Another small pause before fetching
    time.sleep(15)

    df = pytrends.interest_over_time()

    if df.empty:
        print(f"  No data returned for '{keyword}'")
        return []

    # Drop the isPartial column Google adds
    df = df.drop(columns=["isPartial"], errors="ignore")
    df = df.reset_index()

    records = []
    for _, row in df.iterrows():
        records.append({
            "date":     row["date"].strftime("%Y-%m-%d"),
            "interest": int(row[keyword]),
        })

    print(f"  Got {len(records)} data points")
    return records


# 2: Save raw JSON to disk
def save_raw_json(keyword: str, records: list[dict]) -> str:

    today = datetime.utcnow().strftime("%Y-%m-%d")
    folder = os.path.join(RAW_DIR, today)
    os.makedirs(folder, exist_ok=True)

    safe_keyword = keyword.replace(" ", "_").lower()
    filepath = os.path.join(folder, f"trends_{safe_keyword}.json")

    payload = {
        "keyword":      keyword,
        "extracted_at": datetime.utcnow().isoformat(),
        "source":       "google_trends",
        "data":         records,
    }

    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"  Saved JSON → {filepath}")
    return filepath


# 3: Insert into PostgreSQL
def load_to_postgres(keyword: str, records: list[dict]):
    """
    Inserts records into raw_trends table.
    Skips duplicates using ON CONFLICT DO NOTHING.
    """
    if not records:
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    inserted = 0
    for record in records:
        cur.execute(
            """
            INSERT INTO raw_trends (keyword, date, interest, extracted_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
            """,
            (keyword, record["date"], record["interest"]),
        )
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"  Inserted {inserted} rows into raw_trends")


# Main
def run():
    print(f"\n{'='*50}")
    print(f"Trends extract — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Keywords: {KEYWORDS}")
    print(f"{'='*50}\n")

    for keyword in KEYWORDS:
        print(f"[{keyword}]")
        records = fetch_trends(keyword)
        if records:
            save_raw_json(keyword, records)
            load_to_postgres(keyword, records)
        print()
        time.sleep(30)  # Wait before next keyword to avoid hitting Google too fast

    print("Done.\n")


if __name__ == "__main__":
    run()