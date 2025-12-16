from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job3_daily_summary",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,

) as dag:

    run_analytics = BashOperator(
        task_id="compute_daily_summary",
        bash_command="PYTHONPATH=/opt/airflow/src python /opt/airflow/src/job3_analytics.py",
        execution_timeout=timedelta(minutes=2),
    )
