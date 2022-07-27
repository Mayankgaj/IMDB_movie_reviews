from IMDB.entity.artifact_entity import DataValidationArtifact, DataIngestionArtifact
from IMDB.entity.config_entity import DataValidationConfig
from IMDB.exception import IMDBException
from IMDB.util.util import read_yaml_file
from IMDB.logger import logging
import pandas as pd
import os, sys


class DataValidation:

    def __init__(self, data_validation_config: DataValidationConfig,
                 data_ingestion_artifact: DataIngestionArtifact):
        try:
            logging.info(f"{'>>' * 20}Data Validation log started.{'<<' * 20} ")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise IMDBException(e, sys) from e

    def get_train_and_test_df(self):
        try:
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df, test_df
        except Exception as e:
            raise IMDBException(e, sys) from e

    def is_train_test_file_exists(self) -> bool:
        try:
            logging.info("Checking if training and test file is available")

            is_train_file_exist = os.path.exists(self.data_ingestion_artifact.train_file_path)
            is_test_file_exist = os.path.exists(self.data_ingestion_artifact.test_file_path)

            is_available = is_train_file_exist and is_test_file_exist

            logging.info(f"Is train and test file exists?-> {is_available}")

            if not is_available:
                training_file = self.data_ingestion_artifact.train_file_path
                testing_file = self.data_ingestion_artifact.test_file_path
                message = f"Training file: {training_file} or Testing file: {testing_file}" \
                          "is not present"
                raise Exception(message)

            return is_available
        except Exception as e:
            raise IMDBException(e, sys) from e

    def validate_dataset_schema(self) -> bool:
        try:
            config = read_yaml_file(self.data_validation_config.schema_file_path)
            len_col = config["number_of_column"]
            names_of_columns = list(config["columns"].keys())
            target_column = config["target_column"]
            train_df, test_df = self.get_train_and_test_df()

            # Checking train file

            logging.info("Validating Train file  with Schema file,"
                         f"Train file path {self.data_ingestion_artifact.train_file_path}"
                         f"Schema file path {self.data_validation_config.schema_file_path}")

            # Check number of columns in train file
            len_col_train = len(train_df.columns)
            if len_col == len_col_train:
                train_col_no_checked: bool = True
                logging.info("length of columns of Train file is equal to length of columns in schema config")
            else:
                train_col_no_checked: bool = False
                logging.error("length of columns of Train file is equal to length of columns in schema config"
                              f"length of columns in train file is {len_col_train}, required length is {len_col}")

            # Check column names in train file
            col_names = list(train_df.columns.str.rstrip()[:])
            if names_of_columns == col_names:
                train_col_name_checked: bool = True
                logging.info("columns name matching with schema config in train file")
            else:
                train_col_name_checked: bool = False
                logging.error("columns name not matching with schema config in train file")

            # Check target column in train file
            target_column_name = train_df.columns[-1]
            if target_column == target_column_name:
                train_target_col_checked: bool = True
                logging.info("train file target column match with schema file")
            else:
                train_target_col_checked: bool = False
                logging.error("train file target column does not match with schema file")

            train_checked = train_col_no_checked and train_target_col_checked and train_col_name_checked

            # test file

            logging.info("Validating Test file  with Schema file"
                         f"Test file path {self.data_ingestion_artifact.test_file_path}"
                         f"Schema file path {self.data_validation_config.schema_file_path}")

            # Check number of columns in test file
            len_col_test = len(test_df.columns)
            if len_col == len_col_test:
                test_col_no_checked: bool = True
                logging.info("length of columns of Test file is equal to length of columns in schema config")
            else:
                test_col_no_checked: bool = False
                logging.error("length of columns of Test file is not equal to length of columns in schema config"
                              f"length of columns in test file is {len_col_test}, required length is {len_col}")

            # Check column names in test file
            col_names = list(test_df.columns.str.rstrip()[:])
            if names_of_columns == col_names:
                test_col_name_checked: bool = True
                logging.info("columns name matching with schema config in test file")
            else:
                test_col_name_checked: bool = False
                logging.error("columns name not matching with schema config in test file")

            # Check target column in test file
            target_column_name = test_df.columns[-1]
            if target_column == target_column_name:
                test_target_col_checked: bool = True
                logging.info("test file target column match with schema file")
            else:
                test_target_col_checked: bool = False
                logging.error("test file target column does not match with schema file")

            test_checked = test_col_no_checked and test_target_col_checked and test_col_name_checked
            validation_status = train_checked and test_checked

            return validation_status
        except Exception as e:
            raise IMDBException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()

            data_validation_artifact = DataValidationArtifact(
                schema_file_path=self.data_validation_config.schema_file_path,
                is_validated=True,
                message="Data Validation performed successfully."
            )
            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise IMDBException(e, sys) from e

    def __del__(self):
        logging.info(f"{'>>' * 20}Data Validation log completed.{'<<' * 20} \n\n")
