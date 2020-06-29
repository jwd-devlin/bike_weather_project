from airflow.models import BaseOperator, Variable
from airflow.utils import apply_defaults
from fetch_source_data.scripts.weather_data_fetcher import WeatherDataNYCFetcher


class WeatherDataNYCFetcherOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super(WeatherDataNYCFetcherOperator, self).__init__(*args, **kwargs)
        self.file_location = Variable.get("location")
        self.api_key = "_ADD_API_HERE_"

    def execute(self, context):
        WeatherDataNYCFetcher(self.api_key, self.file_location).create_weather_data_csv()
