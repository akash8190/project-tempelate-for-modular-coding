import os, sys, yaml

from src.exception import CustomException
from src.logger import logging

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path, 'rb') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(e)
        raise CustomException(e)