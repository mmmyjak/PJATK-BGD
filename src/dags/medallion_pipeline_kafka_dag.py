from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "bgd-projekt",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

TASK_ENV = {
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "bgd_db"),
}


with DAG(
    dag_id="medallion_airline_pipeline_kafka",
    description="Bronze -> Silver -> Gold airline pipeline triggered from Kafka events",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airlines", "spark", "medallion", "kafka"],
) as dag:
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="docker exec -e TARGET_FILE='{{ dag_run.conf.get(\"file_name\", \"\") }}' -w /app/src bgd_pyspark python -u bronze_ingest.py",
        execution_timeout=timedelta(minutes=40),
        env=TASK_ENV,
        append_env=True,
    )

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command="docker exec -w /app/src bgd_pyspark python -u silver_clean.py",
        execution_timeout=timedelta(minutes=60),
        env=TASK_ENV,
        append_env=True,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="docker exec -w /app/dbt bgd_pyspark dbt run --profiles-dir /app/dbt --select gold",
        execution_timeout=timedelta(minutes=30),
        env=TASK_ENV,
        append_env=True,
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="docker exec -w /app/dbt bgd_pyspark dbt test --profiles-dir /app/dbt --select gold",
        execution_timeout=timedelta(minutes=15),
        env=TASK_ENV,
        append_env=True,
    )

    bronze_ingest >> silver_clean >> dbt_run_gold >> dbt_test_gold
