from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job2_clean_store",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["project"],
) as dag:

    run_cleaner = BashOperator(
        task_id="consume_clean_store",
        bash_command="python /opt/airflow/src/job2_cleaner.py",
    )
