from src.components.data_ingestion import DataIngestionConfig, DataIngestion
from src.components.data_validation import DataValidation
class TrainingPipeline:
    def __init__(self):
        self.data_ingestion =DataIngestion()
        self.data_validation = DataValidation()


    def start_training_pipeline(self):
        self.data_ingestion.initiate_data_ingestion()
        self.data_validation.initiate_data_validation()