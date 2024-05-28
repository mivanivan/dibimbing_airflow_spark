from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "ivan",
    "retry_delay": timedelta(minutes=5)
}

spark_dag = DAG(
    dag_id="assignment-batch-processing",
    default_args=default_args,
    schedule_interval='@day'
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit",
    start_date=days_ago(1)
)

Extract = SparkSubmitOperator(
    application="/spark-scripts/spark-assignment.py",
    conn_id="spark_main",
    task_id="spark_submit_task",
    jars="/opt/bitnami/spark/jars/postgresql-42.2.18.jar",
    dag=spark_dag,
)

Load = PostgresOperator(
    task_id="load_to_postgres",
    postgres_conn_id="postgres_main",
    sql="sql/load_data.sql",  
    dag=spark_dag,
)

# Setting the task dependencies
Extract >> Load

