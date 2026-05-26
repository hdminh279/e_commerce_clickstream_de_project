FROM apache/airflow:2.9.0

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.7.3 \
    dbt-duckdb==1.7.1 \
    awscli \
    astronomer-cosmos==1.3.2 \
    confluent_kafka
