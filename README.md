# bike_weather_project

## Introduction:
 #### - Project Business Logic
 Hypothetical product owner has requested, a way to view nyc bike trip data with the daily weather. Currently their 
 interest is only related to the month of May in 2016.
- Bike Data Source: https://www.citibikenyc.com/system-data
- weather data source: https://meteostat.net/en (updated their website very cool UI)

## Project Plan Breakdown:

Enviroment:
- Apache Airflow ( plus postgres ):  postgres:9.6
- Apache Spark: bitnami/spark:spark:2.4.5-debian-10-r75
- Juypter Notebook: jupyter/pyspark-notebook:latest
- Mongodb: bitnami/mongodb:latest




## File Structure
  - Airflow
     - dag_name
       - operators
       - scripts
  - docker files
  - spark
     - spark resources
  - notebooks_testing
   
## Project Planning thoughts:
Initial environment set up from: https://github.com/cordon-thiago/airflow-spark

- Data Pipeline Orchestration: Options
    - Apache Airflow
    - Apache NiFi
    - Apache Oozie
 
- Data Handling



- Data Storage
    - Mongodb
    - Postgresql
    