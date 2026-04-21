FROM apache/airflow:2.9.0

USER airflow

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-duckdb \
    awscli