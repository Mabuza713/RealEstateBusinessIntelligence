from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

from fetch_state import run_extract

SOURCES = [
    ("real_estate", "extract_real_estate"),
    ("population", "extract_population"),
    ("life_cost", "extract_life_cost"),
    ("overpass", "extract_overpass"),
]

with DAG(
    dag_id="ETL_Pipeline_DAG",
    default_args={"owner": "Mabuza", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Ekstrakcja → Spark staging",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=["etl"],
) as dag:

    def _extract(source_id: str, **_) -> None:
        rows_ok, rows_bad = run_extract(source_id)
        print(f"[{source_id}] clean={rows_ok}, rejected={rows_bad}")

    extracts = [PythonOperator(task_id=tid, python_callable=_extract, op_kwargs={"source_id": sid}) for sid, tid in SOURCES]

    transform = SparkSubmitOperator(
        task_id="transform",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit",
        application="/opt/airflow/spark-apps/spark_transform.py",
        name="ETL_Transform",
        deploy_mode="client",
        conf={
            "spark.driver.host": "airflow-worker",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
        },
        env_vars={"DATA_ROOT": "/opt/airflow/data"},
    )

    extracts >> transform
