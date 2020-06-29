from zipfile import ZipFile
from io import BytesIO
from urllib.request import urlopen
import pandas as pd
import logging
import os


class CitiBikeDataFetcher:
    """
    Checks for and fetches data if required.

    link: "https://s3.amazonaws.com/tripdata/201605-citibike-tripdata.zip"
    """

    def __init__(self):
        self._bike_data_url = "https://s3.amazonaws.com/tripdata/201605-citibike-tripdata.zip"
        self._bike_data_file_name = "201605-citibike-tripdata.csv"

    def fetch_data_from_url(self, file_path: str):
        if self.__check_for_file():
            logging.info(" File %s found", self._bike_data_file_name)
        else:
            read_url = urlopen(self._bike_data_url).read()
            file = ZipFile(BytesIO(read_url))
            bike_data_csv = file.open("201605-citibike-tripdata.csv")
            df_bike_data = pd.read_csv(bike_data_csv)
            logging.info(" Saving 201605-citibike-tripdata.csv")
            df_bike_data.to_csv(file_path + "201605-citibike-tripdata.csv")

    def __check_for_file(self):
        return os.path.isfile(self._bike_data_file_name)
