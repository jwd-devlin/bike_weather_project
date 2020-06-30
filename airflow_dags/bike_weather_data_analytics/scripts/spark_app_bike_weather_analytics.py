import pyspark
import sys
from pyspark.sql import functions as F


class BikeWeatherAnalyticsCreate:
    def __init__(self, database: str):
        self.database = database
        self.monogdb_ip = "host.docker.internal"
        self.monogdb_details = "mongodb://{ip}/{db}".format(ip=self.monogdb_ip, db=self.database)
        self.spark = pyspark.sql.SparkSession.builder \
            .appName("bike_weather_collection") \
            .config("spark.mongodb.input.uri", self.monogdb_details) \
            .config("spark.mongodb.output.uri", self.monogdb_details) \
           .getOrCreate()
        self.bike_weather_analytics_collection = "average_daily_trip_duration_plus_count"

    def __average_daily_trip_duration_plus_count(self, df_bike_weather_collection: pyspark.sql.DataFrame,
                                                 bike_weather_analytics_collection:str):
        average_daily_bike_clean_data = df_bike_weather_collection.select("_id","weather_date",
                                                                          "tripduration").groupBy("weather_date").agg(
            F.mean('tripduration').alias("avg_trip_duration"), F.countDistinct("_id").alias("num_of_riders"))
        average_daily_bike_clean_data.write.format("mongo").option("collection",
                                                            bike_weather_analytics_collection).mode("append").save()

    def execute(self, bike_weather_collection: str, bike_weather_analytics_collection: str):
        df_bike_weather_collection = self.spark.read.format("mongo").option("collection",
                                                                            bike_weather_collection).load()

        self.__average_daily_trip_duration_plus_count(df_bike_weather_collection, bike_weather_analytics_collection)


if __name__ == '__main__':
    BikeWeatherAnalyticsCreate(database="local").execute(bike_weather_collection=sys.argv[1],
                                                         bike_weather_analytics_collection=sys.argv[2])