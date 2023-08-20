import os, sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
import numpy as np
from src.config.configuration import Configuration
import pickle

class DataValidation:
    def __init__(self,  app_config = Configuration()):
        try:
            self.data_validation_config = app_config.get_data_validation_config()

        except Exception as e:
            raise CustomException(e, sys) from e


    def prepare_data(self):
        try:
            logging.info("Data validation Started, We are reading our dataset")

            ratings = pd.read_csv(self.data_validation_config.ratings_csv_file)
            books = pd.read_csv(self.data_validation_config.books_csv_files)

            logging.info(f" Shape of our ratings data file is : {ratings.shape}")

            logging.info(f" Shape of our ratings data file is : {books.shape}")

            books = books[['ISBN', 'Book-Title', 'Book-Author', "Year-Of-Publication", "Publisher", "Image-URL-L"]]

            books.rename(columns = {'Book-Title': 'title',
                                    'Book-Author':'author',
                                    "Year-Of-Publication":'year',
                                    "Publisher": 'publisher',
                                    "Image-URL-L": "image_url"}, inplace=True)
            
            # "User-ID";"ISBN";"Book-Rating"

            ratings.rename(columns = {"User-ID": 'user_id',
                                      "Book-Rating": 'rating'}, inplace=True)
            
            x = ratings['user_id'].value_counts() > 200
            y = x[x].index
            ratings = ratings[ratings['user_id'].isna(y)]

            # rating as well as our Books
            ratings_with_books = ratings.merge(books, on = 'ISBN')
            number_rating = ratings_with_books.groupby('title')['rating'].count().reset_index()
            number_rating.rename(columns = {'rating':"num_of_rating"}, inplace = True)
            final_rating = ratings_with_books.merge(number_rating, on = 'title')

            final_rating = final_rating[final_rating['num_of_rating'] >= 50]

            final_rating.drop_duplicates(['user_id', 'title'], inplace = True)

            os.makedirs(self.data_validation_config.clean_data_dir, exist_ok=True)
            final_rating.to_csv(os.path.join(self.data_validation_config.clean_data_dir, 'clean_data.csv'), index = False)

            
            os.makedirs(self.data_validation_config.objects_dir, exist_ok=True)
            pickle.dump(final_rating, open(os.path.join(self.data_validation_config.objects_dir,"final_rating.pkl"), "wb"))


        except Exception as e:
            raise CustomException(e, sys) from e
        
    
    def initiate_data_validation(self):
        try:
            logging.info("**************Data Validation log Started**************")

            self.prepare_data()
            logging.info("**************Data Validation log COmpleted**************")
        except Exception as e:
            raise CustomException(e, sys) from e