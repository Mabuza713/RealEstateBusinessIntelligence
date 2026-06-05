from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
from pprint import pformat


default_args = {
    "owner": "Maciej Skorupski",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_Pipeline_DAG",
    default_args=default_args,
    description="DAG used for ETL process in real estate market analysis",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 5),
    catchup=False,
) as dag:
    temp_bash = BashOperator(
        task_id="temp_task",
        bash_command="date",
    )

    temp_bash