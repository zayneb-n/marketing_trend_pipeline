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

    # Check 1: NewsAPI
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

    print(f"NewsAPI OK (status {response.status_code})")

    print("\nGoogle Trends pre-check skipped — rate limited, extract_trends will handle retries")
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
    Task 4 — runs AFTER extractions, BEFORE dbt.
    Soft-warns if trends are missing.
    Hard-fails only if news is missing.
    This allows the pipeline to continue when Google Trends is rate-limited.
    """
    import psycopg2

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Check raw_trends
    cur.execute("""
        SELECT COUNT(*) FROM raw_trends
        WHERE extracted_at::date = CURRENT_DATE
    """)
    trends_count = cur.fetchone()[0]

    # Check raw_articles
    cur.execute("""
        SELECT COUNT(*) FROM raw_articles
        WHERE extracted_at::date = CURRENT_DATE
    """)
    news_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Soft warning only for trends
    if trends_count == 0:
        print("WARNING: No trends data today — Google may be rate-limiting. Continuing with news only.")
    else:
        print(f"Trends: {trends_count} rows")

    # Hard fail only for news
    if news_count == 0:
        raise ValueError("No news data extracted today — aborting pipeline before dbt")

    print(f"News: {news_count} rows — proceeding to dbt")


def notify_success(**context):
    """
    Task 7 — final task.
    Logs a completion summary. Swap print for Slack/email in production.
    """
    execution_date = context["execution_date"]
    print(f"""
    Pipeline completed successfully
    --------------------------------
    Date:      {execution_date.strftime("%Y-%m-%d")}
    DAG:       marketing_trend_pipeline
    Schedule:  daily at 08:00 UTC
    Next run:  tomorrow at 08:00 UTC
    --------------------------------
    Full chain:
      check_api_keys
        -> extract_trends + extract_news (parallel)
          -> validate_raw_data
            -> dbt_run
              -> dbt_test
                -> notify_success
    """)


# DAG definition
with DAG(
    dag_id="marketing_trend_pipeline",
    description="Daily extract -> transform pipeline for marketing trends",
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
        trigger_rule="all_done",
    )

    # Task 5: Run dbt models
    task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/transform/marketing_transform && mkdir -p /tmp/dbt-logs /tmp/dbt-target && dbt run --profiles-dir . --log-path /tmp/dbt-logs --target-path /tmp/dbt-target",
    )

    # Task 6: Run dbt tests (changed because dbt failed before executing models because it could not write to its local log file inside the mounted project directory)
    task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/transform/marketing_transform && mkdir -p /tmp/dbt-logs /tmp/dbt-target && dbt test --profiles-dir . --log-path /tmp/dbt-logs --target-path /tmp/dbt-target",
    )

    # Task 7: Notify success
    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # Pipeline order
    task_check_apis >> [task_extract_trends, task_extract_news] >> task_validate >> task_dbt_run >> task_dbt_test >> task_notify


# What we changed:
# 1. validate_raw_data now warns if trends data is missing instead of failing the whole pipeline.
# 2. validate_raw_data still fails if news data is missing, because news is the required source.
# 3. trigger_rule="all_done" was added to validate_raw_data so it runs even if extract_trends fails.
# 4. This makes the pipeline more resilient and allows dbt to continue in a news-only scenario.