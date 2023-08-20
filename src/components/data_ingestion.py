import os, sys
from src.logger import logging
from src.exception import CustomException
from src.entity.config_entity import DataIngestionConfig
from src.constants import *
from src.utils.utils import read_yaml_file
from src.config.configuration import Configuration
import urllib
import requests
import zipfile

class DataIngestion:
    def __init__(self, app_config = Configuration()):
        try:
            self.data_ingestion_config = app_config.get_data_ingestion_config()
        except Exception as e:
            raise CustomException(e, sys)
        
    def download_data(self):
        try:
            dataset_url = self.data_ingestion_config.dataset_download_url
            zip_download_url = self.data_ingestion_config.raw_data_dir
            logging.info(f"Dataset download url: {dataset_url}")
            logging.info(f"Zip download url: {zip_download_url}")
            os.makedirs(zip_download_url, exist_ok=True)
            data_file_name = os.path.basename(dataset_url)
            zip_file_path = os.path.join(zip_download_url, data_file_name)
            logging.info(f"Zip file path: {zip_file_path}")

            urllib.request.urlretrieve(dataset_url, zip_file_path)

            return zip_file_path

        except Exception as e:
            raise CustomException(e, sys)
        
    def extract_zip_file(self, zip_file_path:str):
        try:
            ingested_dir = self.data_ingestion_config.ingested_dir
            os.makedirs(ingested_dir, exist_ok = True)
            with zipfile.ZipFile(zip_file_path, "r") as zip_file:
                zip_file.extractall(ingested_dir)
            logging.info("Extracted zip file : {zip_file_path} into dir {ingested_dir}")

        except Exception as e:
            raise CustomException(e, sys)
        
    def initiate_data_ingestion(self):
        try:
            zip_file_path = self.download_data()
            self.extract_zip_file(zip_file_path= zip_file_path)
            logging.info("Data Ingestion Completed Success")
        except Exception as e:
            raise CustomException(e, sys)

    
