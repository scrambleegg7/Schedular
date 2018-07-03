import numpy as np
import pandas as pd
import os
from datetime import date as dt

import seaborn as sns

import matplotlib.pyplot as plt

import string
import hashlib
import json
import decimal

import csv
import codecs
import boto3
from decimal import *
from pprint import pprint

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


from DecimalEncoderClass import DecimalEncoder

import platform

def connectDynamoDBTable():

    dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
    table = dynamodb.Table('schedular')
    print("TABLE creation date:",table.creation_date_time) 

    return table


class CSVWriter():
    def __init__(self,csv_filename="schedular.csv"):

        print("Running platform:", platform.system() )
        if platform.system() == 'Windows':
            data_dir = "\\Users\\miyuk_000\\Documents\\myData/Miyuki"
        else:
            data_dir = "/Volumes/myShare/ipython"
        
        self.filename = os.path.join(data_dir,csv_filename)

        print("filename for csv output-->",self.filename)

    def writeCSV(self,rows):
        
        with open(self.filename, 'w') as f:
            writer = csv.DictWriter(f, rows[0].keys()) #header
            #writer = csv.DictWriter(f) # without header
            
            writer.writeheader()
            writer.writerows(rows)

        print("done.")
        print("Total row length of writing CSV", len(rows))

class Exporter: 
    def __init__(self, row_limit=10000, profile_name=None): 

        self.row_limit = row_limit
        #if profile_name != '': 
        #    boto3.setup_default_session(profile_name=profile_name) 
        self.client = boto3.client('dynamodb',endpoint_url='http://localhost:8000') 
        #self.resource = boto3.resource('dynamodb') 
        self.resource = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
  
    def get_rows(self, table_name, rows=[], last_evaluated_key=None): 
        response = self.client.scan( 
            TableName=table_name,
            Limit=self.row_limit,
            **({"ExclusiveStartKey": last_evaluated_key} 
            if last_evaluated_key else {}) 
        ) 

        #table = self.resource.Table('schedular')
        #print("total table count for schedular", table.item_count)

        #jd = json.dumps

        rows = rows + response['Items'] 
        # for testing recursion on small tables (< 10000 (default) rows) 
        # if 'LastEvaluatedKey' in response and len(rows) < 100000000000: 
        if 'LastEvaluatedKey' in response and len(rows) < self.row_limit: 
            return rows, response['LastEvaluatedKey'] 
        else: 
            return rows, None 

def main():

    exporter = Exporter()
    csvWriter = CSVWriter()

    table_name = "schedular"
    print("-" * 30)
    print("Table name ", table_name)
    rows, lastEvaluatedKey = exporter.get_rows(table_name) 

    #csvWriter.writeCSV(rows)        
    #print(rows[0])

    rows_test = str( rows[0] ).replace("'", '"')


    df = pd.io.json.json_normalize(rows)

    print(df.columns)
    print(df["name.S"].head())


    
if __name__ == "__main__":
    main()