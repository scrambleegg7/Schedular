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
    table = dynamodb.Table('schedular')
    print("TABLE creation date:",table.creation_date_time) 

    return table

def main():

    table = connectDynamoDBTable()

    data_dir = "L:\\Recepty"
    receptyCls = ReceiptyClass(data_dir)

    df_merge = receptyCls.receiptyFlatten()
    print("* shape of tokyo/national integrated date", df_merge.shape)
    
    h,w = df_merge.shape

    counter = 0
    range25 = list( range(h) )    


    ser = df_merge.iloc[10,:]
    name = ser[0]
    hospital = ser[2]
    medicine = ser[3]
    nextDate = ser[4].strftime("%Y-%m-%d")
    total_amount = ser[5]
    exp = ser[7].strftime("%Y-%m-%d")
    czDate = ser[9].strftime("%Y-%m-%d")

    #print("-" * 40)
    #print("Search nextDate:",nextDate )
    #response = table.query(
    #    KeyConditionExpression=Key('nextDate').eq(nextDate)
    #)
    #items = response['Items']
    #print(items)

    tKey = nextDate + name + hospital + medicine    

    print(tKey)
    try:
        response = table.get_item(
            Key={
                'tableKey': tKey,
                'name' : name
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print("GetItem succeeded:")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))
        print(item)
        print(item["name"])



if __name__ == "__main__":

    main()
