from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
docker_spark_file_path = "/usr/local/airflow/dags/bike_weather_data_analytics/scripts/"
mongodb_collection_clean_weather_data= "weather_data_clean"
mongodb_collection_clean_transformed_bike_data = "cleaned_transformed_bike_data"
mongodb_collection_bike_weather = "bike_weather"
mongodb_collection_analytics = "average_daily_trip_duration_plus_count"
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
        "bike_weather_data_analytics_pipeline",
        default_args=default_args,
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

bike_weather_collection_job = SparkSubmitOperator(
    task_id="bike_weather_collection",
    application= docker_spark_file_path+"spark_app_bike_weather_collection.py",
    name="bike_weather_collection",
    conn_id="spark_internal",
    verbose=1,
    packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.2",
    conf={"spark.master": spark_master},
    application_args=[ mongodb_collection_clean_transformed_bike_data, mongodb_collection_clean_weather_data,
                       mongodb_collection_bike_weather],
    dag=dag)


bike_weather_analytics_job = SparkSubmitOperator(
    task_id="bike_weather_analytics",
    application= docker_spark_file_path+"spark_app_bike_weather_analytics.py",
    name="bike_weather_analytics",
    conn_id="spark_internal",
    verbose=1,
    packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.2",
    conf={"spark.master": spark_master},
    application_args=[mongodb_collection_bike_weather, mongodb_collection_analytics],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> bike_weather_collection_job >> bike_weather_analytics_job >> end