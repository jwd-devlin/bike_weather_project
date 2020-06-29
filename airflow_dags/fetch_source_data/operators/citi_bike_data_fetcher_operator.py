from airflow.models import BaseOperator, Variable
from airflow.utils import apply_defaults
from fetch_source_data.scripts.citi_bike_data_fetcher import CitiBikeDataFetcher


class CitiBikeDataFetcherOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super(CitiBikeDataFetcherOperator, self).__init__(*args, **kwargs)
        self.file_location = Variable.get("location")

    def execute(self, context):
        CitiBikeDataFetcher().fetch_data_from_url(self.file_location)
