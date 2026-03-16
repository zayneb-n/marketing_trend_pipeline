import os
import json
import psycopg2
from datetime import datetime, timedelta
from newsapi import NewsApiClient
from dotenv import load_dotenv

# ── Load environment variables from .env ──
load_dotenv()

# ── Config ────────────────────────────────
KEYWORDS = os.getenv("TREND_KEYWORDS", "AI marketing").split(",")
KEYWORDS = [k.strip() for k in KEYWORDS]

NEWS_API_KEY = os.getenv("NEWS_API_KEY")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", 5432),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

RAW_DIR = os.getenv("RAW_DIR", "./data/raw/news")

# NewsAPI free tier only allows articles from the past 30 days
DAYS_BACK = 7


# ── Step 1: Pull articles from NewsAPI ────
def fetch_news(keyword: str) -> list[dict]:
    """
    Returns a list of article dicts for the given keyword.
    Fetches articles from the past DAYS_BACK days.
    """
    if not NEWS_API_KEY:
        raise ValueError("NEWS_API_KEY is missing from your .env file")

    print(f"  Fetching news for: '{keyword}'")

    newsapi = NewsApiClient(api_key=NEWS_API_KEY)

    from_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    to_date   = datetime.utcnow().strftime("%Y-%m-%d")

    response = newsapi.get_everything(
        q=keyword,
        from_param=from_date,
        to=to_date,
        language="en",
        sort_by="publishedAt",
        page_size=50,           # max per request on free tier
    )

    if response["status"] != "ok":
        print(f"  NewsAPI error: {response.get('message', 'unknown error')}")
        return []

    articles = response.get("articles", [])
    print(f"  Got {len(articles)} articles")

    records = []
    for article in articles:
        records.append({
            "title":        article.get("title"),
            "source_name":  article.get("source", {}).get("name"),
            "published_at": article.get("publishedAt"),
            "url":          article.get("url"),
            "description":  article.get("description"),
        })

    return records


# ── Step 2: Save raw JSON to disk ─────────
def save_raw_json(keyword: str, records: list[dict]) -> str:
    """
    Saves raw articles to data/raw/news/YYYY-MM-DD/news_<keyword>.json
    Returns the file path.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    folder = os.path.join(RAW_DIR, today)
    os.makedirs(folder, exist_ok=True)

    safe_keyword = keyword.replace(" ", "_").lower()
    filepath = os.path.join(folder, f"news_{safe_keyword}.json")

    payload = {
        "keyword":      keyword,
        "extracted_at": datetime.utcnow().isoformat(),
        "source":       "newsapi",
        "articles":     records,
    }

    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"  Saved JSON → {filepath}")
    return filepath


# ── Step 3: Insert into PostgreSQL ────────
def load_to_postgres(keyword: str, records: list[dict]):
    """
    Inserts articles into raw_articles table.
    Skips duplicates based on url using ON CONFLICT DO NOTHING.
    """
    if not records:
        return

    # Add unique constraint on url if not already there
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Ensure unique constraint exists on url column
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'raw_articles_url_key'
            ) THEN
                ALTER TABLE raw_articles ADD CONSTRAINT raw_articles_url_key UNIQUE (url);
            END IF;
        END $$;
    """)
    conn.commit()

    inserted = 0
    for article in records:
        # Parse published_at — NewsAPI returns ISO format string
        published_at = None
        if article.get("published_at"):
            try:
                published_at = datetime.strptime(
                    article["published_at"], "%Y-%m-%dT%H:%M:%SZ"
                )
            except ValueError:
                published_at = None

        cur.execute(
            """
            INSERT INTO raw_articles
                (keyword, title, source_name, published_at, url, description, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (url) DO NOTHING
            """,
            (
                keyword,
                article.get("title"),
                article.get("source_name"),
                published_at,
                article.get("url"),
                article.get("description"),
            ),
        )
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"  Inserted {inserted} rows into raw_articles")


# ── Main ──────────────────────────────────
def run():
    print(f"\n{'='*50}")
    print(f"News extract — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Keywords: {KEYWORDS}")
    print(f"Fetching last {DAYS_BACK} days of articles")
    print(f"{'='*50}\n")

    for keyword in KEYWORDS:
        print(f"[{keyword}]")
        records = fetch_news(keyword)
        if records:
            save_raw_json(keyword, records)
            load_to_postgres(keyword, records)
        print()

    print("Done.\n")


if __name__ == "__main__":
    run()