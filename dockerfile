FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY extract/requirements.txt /opt/airflow/extract/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/extract/requirements.txt

RUN pip install --no-cache-dir dbt-postgres==1.7.0