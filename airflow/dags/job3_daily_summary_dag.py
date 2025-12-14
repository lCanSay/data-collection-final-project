from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job3_daily_summary",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["project"],
) as dag:

    run_analytics = BashOperator(
        task_id="compute_daily_summary",
        bash_command="python /opt/airflow/src/job3_analytics.py",
    )
