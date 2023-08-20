import os, sys
from src.logger import logging
from src.exception import CustomException
from src.entity.config_entity import DataIngestionConfig
from src.constants import *
from src.utils.utils import read_yaml_file
from src.config.configuration import Configuration
from src.pipeline.pipeline import TrainingPipeline

def main():
    try:
        pipeline = TrainingPipeline()
        pipeline.start_training_pipeline()
    except Exception as e:
        raise CustomException(e, sys)
    
if __name__ == "__main__":
    main()