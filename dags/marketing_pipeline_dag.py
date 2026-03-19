from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Default arguments for all tasks in the DAG
default_args = {
    "owner":            "zeineb",
    "retries":          3,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}


# DB config used by both extract and transform steps
DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "marketing_pipeline",
    "user":     "pipeline_user",
    "password": "pipeline_pass_2026",
}


# Task functions

def check_api_keys():
    """
    Task 1 — runs BEFORE extractions.
    1. Checks NEWS_API_KEY exists and makes a real lightweight test call.
    2. Google Trends check DISABLED — rate limited, extract_trends handles retries.
    """
    import os
    import requests

    print("=" * 50)
    print("API connectivity check")
    print("=" * 50)

    # ── Check 1: NewsAPI ──────────────────
    news_api_key = os.getenv("NEWS_API_KEY")

    if not news_api_key:
        raise ValueError("NEWS_API_KEY is missing from environment — add it to your .env file")

    print("\n[1] Testing NewsAPI key...")
    response = requests.get(
        "https://newsapi.org/v2/top-headlines",
        params={"country": "us", "pageSize": 1},
        headers={"X-Api-Key": news_api_key},
        timeout=10,
    )

    if response.status_code == 401:
        raise ValueError("NewsAPI key is invalid or expired — check NEWS_API_KEY in .env")
    if response.status_code == 429:
        raise ValueError("NewsAPI rate limit hit — pipeline will retry later")
    if response.status_code != 200:
        raise ValueError(f"NewsAPI returned unexpected status {response.status_code}: {response.text}")

    print(f"  NewsAPI OK (status {response.status_code})")

    # ── Check 2: Google Trends ── DISABLED (rate limited by Google)
    # try:
    #     from pytrends.request import TrendReq
    #     import time
    #     time.sleep(3)
    #     pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25), retries=2, backoff_factor=0.5)
    #     pytrends.build_payload(["marketing"], timeframe="today 1-m")
    #     time.sleep(5)
    #     df = pytrends.interest_over_time()
    #     if df is None:
    #         raise ValueError("Google Trends returned None — possible block or rate limit")
    #     print(f"  Google Trends OK ({len(df)} data points returned in test)")
    # except Exception as e:
    #     raise ValueError(
    #         f"Google Trends connectivity failed: {e}\n"
    #         f"  → Google may be rate-limiting your IP. Wait 30 min and retry."
    #     )

    print("\n  ⚠️  Google Trends pre-check skipped — rate limited, extract_trends will handle retries")
    print("\nAll API checks passed — starting extraction\n")


def run_extract_trends():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "extract_trends",
        "/opt/airflow/extract/extract_trends.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run()


def run_extract_news():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "extract_news",
        "/opt/airflow/extract/extract_news.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run()


def validate_raw_data():
    """
    Task 4 — runs AFTER both extractions, BEFORE dbt.
    Aborts if either table has no rows for today.
    Catches silent API failures that returned 200 but inserted nothing.
    """
    import psycopg2

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # ── Check raw_trends ─────────────────
    cur.execute("""
        SELECT COUNT(*) FROM raw_trends
        WHERE extracted_at::date = CURRENT_DATE
    """)
    trends_count = cur.fetchone()[0]

    # ── Check raw_articles ───────────────
    cur.execute("""
        SELECT COUNT(*) FROM raw_articles
        WHERE extracted_at::date = CURRENT_DATE
    """)
    news_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    if trends_count == 0:
        raise ValueError("No trends data extracted today — aborting pipeline before dbt")
    if news_count == 0:
        raise ValueError("No news data extracted today — aborting pipeline before dbt")

    print(f"Validation passed: {trends_count} trend rows, {news_count} news rows extracted today")


def notify_success(**context):
    """
    Task 7 — final task.
    Logs a completion summary. Swap print for Slack/email in production.
    """
    execution_date = context["execution_date"]
    print(f"""
    Pipeline completed successfully
    ─────────────────────────────────
    Date:      {execution_date.strftime("%Y-%m-%d")}
    DAG:       marketing_trend_pipeline
    Schedule:  daily at 08:00 UTC
    Next run:  tomorrow at 08:00 UTC
    ─────────────────────────────────
    Full chain:
      check_api_keys
        → extract_trends + extract_news (parallel)
          → validate_raw_data
            → dbt_run
              → dbt_test
                → notify_success
    """)


# ── DAG definition ────────────────────────
with DAG(
    dag_id="marketing_trend_pipeline",
    description="Daily extract → transform pipeline for marketing trends",
    default_args=default_args,
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["marketing", "trends", "dbt"],
) as dag:

    # Task 1: Check APIs
    task_check_apis = PythonOperator(
        task_id="check_api_keys",
        python_callable=check_api_keys,
    )

    # Task 2: Extract trends
    task_extract_trends = PythonOperator(
        task_id="extract_trends",
        python_callable=run_extract_trends,
    )

    # Task 3: Extract news
    task_extract_news = PythonOperator(
        task_id="extract_news",
        python_callable=run_extract_news,
    )

    # Task 4: Validate raw data
    task_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
    )

    # Task 5: Run dbt models
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/transform/marketing_transform && dbt run --profiles-dir .",
    )

    # Task 6: Run dbt tests
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/transform/marketing_transform && dbt test --profiles-dir .",
    )

    # Task 7: Notify success
    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ── Pipeline order ────────────────────
    task_check_apis >> [task_extract_trends, task_extract_news] >> task_validate >> task_dbt_run >> task_dbt_test >> task_notify