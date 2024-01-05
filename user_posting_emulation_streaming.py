import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import datetime
import json

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
    Then sends this data to an API, using an invoke url through a PUT request, to send the data to three AWS Kinesis streams. 
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
            
            invoke_url_pin = "https://5i08sjvi96.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12b287eedf6d-pin/record"

            pin_data = json.dumps({
                "StreamName": "streaming-12b287eedf6d-pin",
                "Data": {
                        "index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], 
                        "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": 
                        pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
                        "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"],
                        "category": pin_result["category"]
                        },
                        "PartitionKey": "test"
                        })

            headers = {'Content-Type': 'application/json'}
            pin_response = requests.request("PUT", invoke_url_pin, headers=headers, data=pin_data)

            invoke_url_geo = "https://5i08sjvi96.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12b287eedf6d-geo/record"

            geo_data = json.dumps({
                "StreamName": "streaming-12b287eedf6d-geo",
                "Data": {
                        "ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], 
                        "longitude": geo_result["longitude"], "country": geo_result["country"]
                        },
                        "PartitionKey": "test"
                        }
                        , default=str)   

            headers = {'Content-Type': 'application/json'}
            geo_response = requests.request("PUT", invoke_url_geo, headers=headers, data=geo_data)

            invoke_url_user = "https://5i08sjvi96.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12b287eedf6d-user/record"

            user_data = json.dumps({
                "StreamName": "streaming-12b287eedf6d-user",
                "Data": {
                        "ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], 
                        "age": user_result["age"], "date_joined": user_result["date_joined"]
                        },
                        "PartitionKey": "test"
                        }
                        , default=str)   

            headers = {'Content-Type': 'application/json'}
            user_response = requests.request("PUT", invoke_url_user, headers=headers, data=user_data)

            print(pin_response.status_code)
            print(geo_response.status_code)
            print(user_response.status_code)

if __name__ == "__main__":
    run_infinite_post_data_loop()