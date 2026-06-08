from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

from fetch_state import run_extract

SOURCES = [
    ("real_estate", "extract_real_estate"),
    ("population", "extract_population"),
    ("overpass", "extract_overpass"),
]

_SPARK_CONF = {
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
}

_SPARK_ENV = {"DATA_ROOT": "/opt/airflow/data"}
_APPS_DIR = "/opt/airflow/spark-apps"


def _extract(source_id: str, **_) -> None:
    rows_ok = run_extract(source_id)
    print(f"[{source_id}] clean={rows_ok}")


def _spark_op(task_id: str, script: str, name: str) -> SparkSubmitOperator:
    """Return a configured SparkSubmitOperator for the given script."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit",
        application=f"{_APPS_DIR}/{script}",
        name=name,
        deploy_mode="client",
        
        # Zgodność z zasobami ze spark-defaults.conf w Twoim Dockerfile
        executor_cores=4,
        executor_memory="4g",
        driver_memory="4g",
        
        conf=_SPARK_CONF,
        env_vars=_SPARK_ENV,
    )


with DAG(
    dag_id="ETL_Pipeline_DAG",
    default_args={"owner": "Mabuza", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Extract -> Spark staging -> Spark clean -> Spark load",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=["etl"],
) as dag:

    extracts = [
        PythonOperator(task_id=tid, python_callable=_extract, op_kwargs={"source_id": sid})
        for sid, tid in SOURCES
    ]

    transform = _spark_op("transform", "spark_transform.py", "ETL_Transform")
    clean = _spark_op("clean", "spark_clean.py", "ETL_Clean")
    measures = _spark_op("measures", "spark_measures.py", "ETL_Measures")
    load = _spark_op("load", "spark_load.py", "ETL_Load")

    extracts >> transform >> clean >> measures >> load
