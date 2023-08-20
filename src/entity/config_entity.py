from collections import namedtuple

DataIngestionConfig = namedtuple("DatasetConfig", ['dataset_download_url',
                                                   'raw_data_dir',
                                                   'ingested_dir'])


DataValidationConfig = namedtuple("DataValidationConfig",['clean_data_dir',
                                                          'objects_dir',
                                                          'books_csv_files',
                                                          'ratings_csv_file'])

