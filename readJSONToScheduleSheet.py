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
import pandas 
from pandas.io.json import json_normalize

import codecs
import boto3
from decimal import *

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


from DecimalEncoderClass import DecimalEncoder

import platform


class JSONReader():
    def __init__(self,csv_filename="schedular.csv"):

        print("Running platform:", platform.system() )
        if platform.system() == 'Windows':
            data_dir = "\\Users\\miyuk_000\\Documents\\myData/Miyuki"
        else:
            data_dir = "/Volumes/myShare/ipython"
        
        self.filename = os.path.join(data_dir,csv_filename)

        print("JSON filename -->",self.filename)

    def readJSON(self):

        pd.set_option("display.max_colwidth", 1024)
        
        with open(self.filename) as f:
            lines = f.readlines()
 
            json_lines = map(lambda l: json.loads( l.replace("'", '"') ), lines)
            
            df = pd.io.json.json_normalize(list(json_lines))
            
            print(df.head(10))

        print("done.")
        print("Total row length of JOSN", df.shape[0])


    def readJSON2(self):
        with open(self.filename) as f:
            reader = csv.DictReader(f, fieldnames=None)
            idx = 0
            for row in reader:
                if idx > 0:
                    newdf = pd.read_json( row.replace("'", '"') )
                idx += 1
                #pd.io.json.json_normalize(list(row))
                #print(row["name"]["S"])

            #print( json.dumps(jsonData, sort_keys = True, indent = 4) )








def main():

    jsonreader = JSONReader()
    jsonreader.readJSON()



if __name__ == "__main__":
    main()