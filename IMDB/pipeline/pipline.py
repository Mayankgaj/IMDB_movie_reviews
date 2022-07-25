from IMDB.config.configuration import Configuration
from IMDB.exception import IMDBException
from IMDB.entity.artifact_entity import DataIngestionArtifact
from IMDB.component.data_ingestion import DataIngestion
import sys


class Pipeline:

    def __init__(self, config: Configuration = Configuration()) -> None:
        try:
            self.config = config

        except Exception as e:
            raise IMDBException(e, sys) from e

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise IMDBException(e, sys) from e

    def run_pipeline(self):
        try:
            # data ingestion
            self.start_data_ingestion()

        except Exception as e:
            raise IMDBException(e, sys) from e
        