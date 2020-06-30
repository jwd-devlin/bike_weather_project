import pyspark.sql.functions as F
from pyspark.sql import types as t
import pyspark
import datetime
import logging
import sys


class TransformBikeDataToMongodb:
    """

    using spark send the data to or from mongodb

    """

    def __init__(self, database: str, collection_input: str, collection_output: str):
        self.database = database
        self.collection_input = collection_input
        self.collection_output = collection_output
        self.monogdb_ip = "host.docker.internal"
        self.monogdb_input = "mongodb://{ip}/{db}.{col}".format(ip=self.monogdb_ip, db=self.database,
                                                                col=self.collection_input)
        self.monogdb_output = "mongodb://{ip}/{db}.{col}".format(ip=self.monogdb_ip, db=self.database,
                                                                col=self.collection_output)
        self.spark = pyspark.sql.SparkSession.builder\
                          .appName("transform_weather_data")\
                          .config("spark.mongodb.input.uri", self.monogdb_input )\
                          .config("spark.mongodb.output.uri",self.monogdb_output )\
                          .getOrCreate()
        self.cleaned_date_format = "%m/%d/%Y %H:%M:%S"
        self.key_bike_data_monogdb_query = "{},"

    def check_if_data_frame_empty(self, df : pyspark.sql.DataFrame):
        if df.rdd.isEmpty():
            logging.info("Data imported correctly", df.show(2))
        else:
            logging.info("Data not found", df.show(2))


    @staticmethod
    @F.udf(t.StringType())
    def __reformat_starttime_date_for_mapping( date: str):
        date = datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S")
        return date.strftime('%Y-%m-%d')

    def __transform_date_data(self, df_cleaned_bike_data: pyspark.sql.DataFrame):
        return df_cleaned_bike_data.withColumn("weather_date", self.__reformat_starttime_date_for_mapping("starttime"))

    def execute(self):
        logging.info("Transforming data from %s, outputting into %s", self.collection_input, self.collection_output)

        # load data
        df_cleaned_data = self.spark.read.format("mongo").load()

        self.check_if_data_frame_empty(df_cleaned_data)

        # check date quality
        df_cleaned_normalised_date= self.__transform_date_data(df_cleaned_data)

        df_average_normalised_date_daily= df_cleaned_normalised_date.select("weather_date", "tripduration").groupBy(
            "weather_date").agg(F.mean('tripduration'))

        # load into mongodb
        df_average_normalised_date_daily.write.format("mongo").mode("append").save()

        self.spark.stop()


if __name__ == '__main__':

    TransformBikeDataToMongodb(database = "local", collection_input = sys.argv[1], collection_output = sys.argv[2]
                               ).execute()