from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
bike_data_file_path = "/usr/local/spark/resources/data/201605-citibike-tripdata.csv"
docker_spark_file_path = "/usr/local/airflow/dags/bike_data_etl/scripts/"
mongodb_collection_clean_bike_data= "cleaned_bike_data"
mongodb_collection_clean_transformed_bike_data = "cleaned_transformed_bike_data"
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
        "bike_data_pipeline",
        default_args=default_args,
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

bike_extraction_clean_data_job = SparkSubmitOperator(
    task_id="extract_clean_bike_data",
    application= docker_spark_file_path+"spark_app_extract_bike_clean_data_to_mongodb.py",
    name="extract_bike_data",
    conn_id="spark_internal",
    verbose=1,
    packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.2",
    conf={"spark.master":spark_master},
    application_args=[bike_data_file_path, mongodb_collection_clean_bike_data],
    dag=dag)

transform_clean_bike_data_job = SparkSubmitOperator(
    task_id="transform_clean_bike_data",
    application= docker_spark_file_path+"spark_app_transform_bike_data_to_mongodb.py",
    name="transform_bike_data",
    conn_id="spark_internal",
    verbose=1,
    packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.2",
    conf={"spark.master":spark_master},
    application_args=[mongodb_collection_clean_bike_data, mongodb_collection_clean_transformed_bike_data],
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

start >> bike_extraction_clean_data_job >> transform_clean_bike_data_job >> end