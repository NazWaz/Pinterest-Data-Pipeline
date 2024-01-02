import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)

class AWSDBConnector:
    '''
    A connector to an AWS database containing Pinterest data.

    Attributes:
    ----------
    HOST: str
        The host url.
    USER : str
        The username needed to access the database.
    PASSWORD : str
        The user's password needed to access the database.
    DATABASE: str
        The name of the database.
    PORT: int
        The port number.

    Methods:
    -------
    create_db_connector
        Creates connection to the AWS database containing pinterest post and user data.
    '''
    def __init__(self):
        '''
        Constructs all the neccessary attributes for the AWSDBConnector object.
        '''

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        '''
        Uses the database type (mysql) and database api (pymysql) along with the previous attributes (host, user, password, database and port)
        together to create a connection to an AWS database containing Pinterest data.
        '''

        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    '''
    Runs an infinite post data loop to continuously receive and output data into three tables.
    These tables are then output as three dictionaries of key value pairs with the headings as the key and the data as the values.
    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
