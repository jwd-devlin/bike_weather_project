import pyspark
import sys


class BikeWeatherCollectionCreate:
    def __init__(self, database: str):
        self.database = database
        self.monogdb_ip = "host.docker.internal"
        self.monogdb_details = "mongodb://{ip}/{db}".format(ip=self.monogdb_ip, db=self.database)
        self.spark = pyspark.sql.SparkSession.builder \
            .appName("bike_weather_collection") \
            .config("spark.mongodb.input.uri", self.monogdb_details) \
            .config("spark.mongodb.output.uri", self.monogdb_details) \
           .getOrCreate()

    def execute(self, bike_collection: str, weather_collection:str , bike_weather_collection:str):
        df_bike_collection = self.spark.read.format("mongo").option("collection", bike_collection).load()
        bike_count = df_bike_collection.count()
        df_weather_collection =  self.spark.read.format("mongo").option("collection", weather_collection).load()
        weather_count = df_weather_collection.count()
        print(" Weather df size ",weather_count , " , bike df ", bike_count)
        #join
        df_bike_weather = df_bike_collection.join(df_weather_collection, df_bike_collection.weather_date == df_weather_collection.date, "inner")
        bike_weather_count = df_bike_weather.count()
        print(" Weather and bike count ", bike_weather_count)
        df_bike_weather.write.format("mongo").option("collection", bike_weather_collection).mode("append").save()


if __name__ == '__main__':
    BikeWeatherCollectionCreate(database="local").execute(bike_collection=sys.argv[1], weather_collection=sys.argv[2], bike_weather_collection = sys.argv[3])