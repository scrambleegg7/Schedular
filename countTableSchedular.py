
import numpy as np
import pandas as pd
import os
import shutil
from datetime import date as dt

import seaborn as sns

import matplotlib.pyplot as plt

import string
import hashlib

import csv
import codecs
import boto3
from decimal import *

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from boto3.dynamodb.conditions import Key, Attr

from botocore.exceptions import ClientError

def touch(path):
    with open(path, 'a'):
        os.utime(path, None)

def connectDynamoDBTable():

    filename = "./dynamo_connectionfail"
    try:
        os.remove(filename)
    except OSError:
        pass


    try:
        dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
        table = dynamodb.Table('schedular')
        print("TABLE creation date:",table.creation_date_time) 
        return table
    except:
        print("Connection Error")
        touch(filename)
        return None
        
        #prin(ce.response)


table = connectDynamoDBTable()

if table is not None:
    print("total table count for schedular", table.item_count)
