from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job1_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",    # every minute
    catchup=False,
    max_active_runs=1,
) as dag:

    run_producer = BashOperator(
        task_id="run_producer_batch",
        bash_command="python /opt/airflow/src/job1_producer.py",
    )
