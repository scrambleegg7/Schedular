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

def searchDataMainKey(table,czDate):

    try:
        response = table.query(
            KeyConditionExpression=Key('tableKey').eq(czDate)
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        #print(response)
        #
        items = response['Items']
        return items



def searchDateFromDynamoDB(table,czDate):

    try:

        response = table.query(
            IndexName='nextDateIndex',
            KeyConditionExpression=Key('czDate').eq(czDate)
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        #print(response)
        #
        items = response['Items']
        return items

def main():

    table = connectDynamoDBTable()

    #data_dir = "/Users/donchan/Documents/myData/miyuki"
    data_dir = "/Volumes/myShare/RECEPTY"
    
    receptyCls = ReceiptyClass(data_dir)

    df_merge = receptyCls.receiptyFlatten()
    print("* shape of tokyo/national integrated date", df_merge.shape)

    print("Exclude Tobunerima Clinic")
    mask = df_merge.hospital.str.contains('北桜') 
    df_merge = df_merge[~mask].sort_values(["nextDate","name"])
    print("* shape after omitting Tobunerima clinic....", df_merge.shape)

    df_merge.nextDate = df_merge.nextDate.dt.strftime("%Y/%m/%d")
    df_merge.czDate = df_merge.czDate.dt.strftime("%Y/%m/%d")
    df_merge.exp = df_merge.exp.dt.strftime("%Y/%m/%d")
     
    #tKey = nextDate + czDate + name + hospital + medicine + str_total_amount



    date_ops = lambda x:pd.datetime.strptime(x,"%Y/%m/%d")    
    date_conv1 = lambda x:pd.datetime.strftime("%Y/%m/%d")        

    czDate = df_merge.czDate.dt.strftime("%Y/%m/%d")
    u, indices = np.unique(czDate, return_index=True)

    EmptyczDate = []
    for unit, cnt in zip(u,indices):

        # search with czDate
        items = searchDateFromDynamoDB( table,unit )

        DataLengthByczDate = len(items)
        if DataLengthByczDate == 0:

            print("-" * 30)
            print("czDate:", unit)
            print("czDate count on dynamoDB: ", len(items))

            df_czSelect = df_merge[df_merge.czDate.dt.strftime("%Y/%m/%d") == unit ].copy()
            print(df_czSelect.shape)

            EmptyczDate.append( unit )
            
    
    emptyczDates = list( map( date_ops, EmptyczDate  )  )
    #print(emptyczDates)
    df_czSelect = df_merge[ df_merge.czDate.isin( emptyczDates ) ].copy()
    print("total selected shape ",df_czSelect.shape)

    print("-"* 40)
    print(" output csv file to import Google Sheets..")
    print( EmptyczDate )
    data_dir = "/Volumes/myShare/ipython"
    integrate_csvfile = os.path.join(data_dir, "integrate2.csv")

    df_czSelect.nextDate = df_czSelect.nextDate.dt.strftime("%Y/%m/%d")
    df_czSelect.czDate = df_czSelect.czDate.dt.strftime("%Y/%m/%d")
    df_czSelect.exp = df_czSelect.exp.dt.strftime("%Y/%m/%d")
     

    df_czSelect.to_csv(integrate_csvfile, index=False, encoding="cp932")



if __name__ == "__main__":

    main()
