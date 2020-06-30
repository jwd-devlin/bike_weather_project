from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
weather_data_file_path = "/usr/local/spark/resources/data/weather_nyc_2016-05-01_2016-06-01.csv"
docker_spark_file_path = "/usr/local/airflow/dags/weather_data_etl/scripts/"
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        "weather_data_pipeline",
        default_args=default_args,
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

extract_clean_weather_job = SparkSubmitOperator(
    task_id="extract_clean_weather_data_to_mongodb",
    application=docker_spark_file_path+"spark_app_extract_weather_clean_data_to_mongodb.py",
    name="extract_clean_weather",
    conn_id="spark_internal",
    verbose=1,
    packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.2",
    conf={"spark.master":spark_master},
    application_args=[weather_data_file_path],
    dag=dag)




end = DummyOperator(task_id="end", dag=dag)

start >>  extract_clean_weather_job >> end