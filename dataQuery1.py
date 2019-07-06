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

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


from DecimalEncoderClass import DecimalEncoder

def connectDynamoDBTable():

    dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
    #dynamodb = boto3.client('dynamodb',endpoint_url='http://localhost:8000')
    
    table = dynamodb.Table('schedular')
    #print("TABLE creation date:",table.creation_date_time) 
    #tables = dynamodb.list_tables()
    #print("Tables List", tables["TableNames"] )

    return table


def main():
    table = connectDynamoDBTable()

    print("all records....")
    result = table.scan()
    print( result['Items'])

if __name__ == "__main__":

    main()
