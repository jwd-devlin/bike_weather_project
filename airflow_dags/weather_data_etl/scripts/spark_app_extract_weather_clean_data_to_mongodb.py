from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql import types as t
import pyspark
from datetime import datetime
import logging
import sys

class WriteWeatherCleanDataToMongodb:
    """

    using spark send the data to or from mongodb

    """

    def __init__(self, database: str, collection: str):
        self.database = database
        self.collection = collection
        self.monogdb_ip = "host.docker.internal"
        self.monogdb_details = "mongodb://{ip}/{db}.{col}".format(ip =self.monogdb_ip,db = self.database, col = self.collection)
        self.spark = pyspark.sql.SparkSession.builder\
                          .appName("clean_weather_data")\
                          .config("spark.mongodb.input.uri", self.monogdb_details )\
                          .config("spark.mongodb.output.uri",self.monogdb_details )\
                          .getOrCreate()

    @staticmethod
    @F.udf(t.BooleanType())
    def __check_date_format(date: str):
        try:
            date_formated = datetime.strptime(date, "%Y-%m-%d")
            return True
        except ValueError:
            logging.info("Date format found to be incorrect %s", date)
            return False

    def __quality_check_date_string_format(self, df: pyspark.sql.DataFrame):
        return df.filter(self.__check_date_format(col("date")))

    def execute(self, file_path:str):
        # load data
        df_weather_data = self.spark.read.csv(file_path, header = True, sep = ",")

        # check date quality
        df_weather_data_clean = self.__quality_check_date_string_format(df_weather_data)

        # load into mongodb
        df_weather_data_clean.write.format("mongo").mode("append").save()

        self.spark.stop()


if __name__ == '__main__':
    logging.info("Using class")
    WriteWeatherCleanDataToMongodb(database = "local", collection = "weather_data_clean").execute( file_path = sys.argv[1])