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
    dag_id="medallion_airline_pipeline",
    description="Bronze -> Silver -> Gold airline pipeline orchestrated by Airflow",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airlines", "spark", "medallion"],
) as dag:
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="docker exec -w /app/src bgd_pyspark python -u bronze_ingest.py",
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

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command="docker exec -w /app/src bgd_pyspark python -u gold_aggregate.py",
        execution_timeout=timedelta(minutes=40),
        env=TASK_ENV,
        append_env=True,
    )

    bronze_ingest >> silver_clean >> gold_aggregate
