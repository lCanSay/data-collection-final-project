from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job1_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="*/2 * * * *",    # every 2 minutes
    catchup=False,
    max_active_runs=1,
    concurrency=1
) as dag:
    BashOperator(
        task_id="poll_api_publish_kafka",
        bash_command="PYTHONPATH=/opt/airflow/src python /opt/airflow/src/job1_producer.py",
        execution_timeout=timedelta(seconds=90),
    )
