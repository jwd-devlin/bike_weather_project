import pandas as pd
import os
import logging
import requests


class WeatherDataNYCFetcher:
    def __init__(self, api_key: str, file_location: str):
        self._nyc_weather_wall_street_station_id = "72502"
        self._weather_api_url_daliy_data = "https://api.meteostat.net/v2/stations/daily"
        self._api_key = api_key
        self._weather_data_file_name = "weather_nyc_{start}_{end}.csv"
        self.file_location = file_location

    def __check_for_file(self, start: str, end: str):
        return os.path.isfile(self._weather_data_file_name.format(start=start, end=end))

    def fetch_weather_data_daily_by_date(self, start: str, end: str):
        """
        weather date format: YYYY-MM-DD
            - 2019-05-01
        """
        headers = {
            'x-api-key': self._api_key
        }

        payload = (('station', self._nyc_weather_wall_street_station_id), ('start', start),
                   ('end', end))
        response = requests.get(self._weather_api_url_daliy_data, headers=headers, params=payload)

        df_weather_data = pd.DataFrame(response.json()["data"])
        file_name = self._weather_data_file_name.format(start=start, end=end)
        df_weather_data.to_csv(self.file_location + file_name)

    def create_weather_data_csv(self, start: str = "2016-05-01", end: str = "2016-06-01"):

        if self.__check_for_file(start, end):
            logging.info(" File %s found", self._weather_data_file_name)
        else:
            self.fetch_weather_data_daily_by_date(start, end)
