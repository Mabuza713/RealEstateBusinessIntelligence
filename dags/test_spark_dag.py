from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Mabuza",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='test_spark_dag',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    submit_job = SparkSubmitOperator(
        task_id='run_pyspark_test',
        conn_id='spark_default',
        spark_binary='/opt/spark/bin/spark-submit',
        application='/opt/airflow/spark-apps/test_spark_job.py',
        name='TestSparkAppFromAirflow',
        verbose=True,
        deploy_mode='client',
        conf={
            'spark.driver.host': 'airflow-worker',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.pyspark.python': 'python3',
            'spark.pyspark.driver.python': 'python3',
        },
    )
    submit_job