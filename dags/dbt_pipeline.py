from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id = "dbt_clickstream",

    default_args = {
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },

    description="dbt Pipeline",
    start_date = datetime(2026, 4, 20),
    schedule='*/30 * * * *',
    catchup=False,
) as dag:
    
    DBT_DIR = '/opt/airflow/ecommerce_dbt'
    TARGET = '--target prod'
    
    t0_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"cd {DBT_DIR} && dbt deps {TARGET}"
    )

    t1_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f"cd {DBT_DIR} && dbt run -s models/staging {TARGET}"
    )

    t2_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test -s models/staging {TARGET}"
    )

    t3_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot {TARGET}"
    )

    t4_test_dim = BashOperator(
        task_id="dbt_test_dim",
        bash_command=f"cd {DBT_DIR} && dbt test -s snapshots {TARGET}"
    )

    t5_run_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=f"cd {DBT_DIR} && dbt run -s models/mart {TARGET}"
    )

    t6_test_mart = BashOperator(
        task_id='dbt_test_mart',
        bash_command=f'cd {DBT_DIR} && dbt test -s models/mart {TARGET}'
    )

    t0_deps >> t1_run_staging >> t2_test_staging >> t3_snapshot >> t4_test_dim >> t5_run_mart >> t6_test_mart
