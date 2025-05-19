from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="spark_ingest_smoking_csv",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Ingest smoking.csv to Postgres via Spark",
    start_date=days_ago(1),
)

ingest_smoking = SparkSubmitOperator(
    application="/spark-scripts/spark-smoking.py",  # Script Spark Anda
    conn_id="spark_main",
    task_id="spark_ingest_smoking_task",
    dag=dag,
    application_args=[
        "/data/smoking.csv"  # Path CSV di cluster Spark
    ],
)

ingest_smoking