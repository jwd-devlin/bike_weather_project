from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from fetch_source_data.operators.weather_data_fetcher_operator import WeatherDataNYCFetcherOperator
from fetch_source_data.operators.citi_bike_data_fetcher_operator import CitiBikeDataFetcherOperator

###############################################
# Parameters
###############################################
file_path = "/usr/local/spark/resources/data/"
weather_api = ""
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "start_date": datetime(now.year, now.month, now.day),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "fetch_raw_data_sources",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

bike = CitiBikeDataFetcherOperator(task_id="bike_data",
                                   provide_context=True,
                                   dag=dag)

weather = WeatherDataNYCFetcherOperator(task_id="weather_data",
                                        provide_context=True,
                                        api_key = weather_api,
                                        dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> weather >> bike >> end
