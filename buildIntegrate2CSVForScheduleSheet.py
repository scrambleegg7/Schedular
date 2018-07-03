import numpy as np
import pandas as pd
import os
import platform


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

def scanFilter(table, czDate , name , hospital, medicine):

    try:
        response = table.scan(
            FilterExpression=Attr('czDate').eq(czDate) & Attr('name').eq(name) & Attr('hospital').eq(hospital) &  \
                            Attr('medicine').eq(medicine) 
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        #print(response)
        #
        items = response['Items']
        return response


def searchDataMainKey(table,tKey,nextDate):

    try:
        response = table.query(
            KeyConditionExpression=Key('tableKey').eq(tKey) & Key('nextDate').eq(nextDate)
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        #print(response)
        #
        items = response['Items']
        return response



def searchDateFromDynamoDB(table,czDate, name):

    try:

        response = table.query(
            IndexName='czDateIndex',
            KeyConditionExpression=Key('czDate').eq(czDate) & Key('name').eq(name)
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
    if platform.system() == "Windows":
        recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/RECEPTY"
    
    receptyCls = ReceiptyClass(recepty_data_dir)

    df_merge = receptyCls.receiptyFlatten()
    print("* shape of tokyo/national integrated date", df_merge.shape)

    print("Exclude Tobunerima Clinic")
    mask = df_merge.hospital.str.contains('北桜') 
    df_merge = df_merge[~mask].sort_values(["nextDate","name"])
    print("* shape after omitting Tobunerima clinic....", df_merge.shape)

    df_merge.nextDate = df_merge.nextDate.dt.strftime("%Y/%m/%d")
    df_merge.czDate = df_merge.czDate.dt.strftime("%Y/%m/%d")
    df_merge.exp = df_merge.exp.dt.strftime("%Y/%m/%d")
     
    df_merge.total_amount = df_merge.total_amount.apply( lambda x:'{:.2f}'.format(x) ) 
    #tKey = nextDate + czDate + name + hospital + medicine + str_total_amount

    notFound = 0
    df_Select = pd.DataFrame(columns = df_merge.columns)
    for i in range( df_merge.shape[0] ):

        row_data = df_merge.iloc[i,:].tolist()
        #hospital = df_merge.ix[i,2]
        name = row_data[0]
        hospital = row_data[2]
        czDate = row_data[9]
        medicine = row_data[3]
        nextDate = row_data[4]
        str_total_amount = str( row_data[5] )

        items = searchDateFromDynamoDB(table,czDate,name)
        if len(items ) == 0:
            df_Select.loc[i] = row_data
            

    #data_dir = "/Volumes/myShare/ipython"
    integrate_csvfile = os.path.join(data_dir, "integrate2.csv")

    print("Selected data having new czDate / name. ",df_Select.shape)
    print("data is saved.", integrate_csvfile)
    df_Select.to_csv(integrate_csvfile, index=False, encoding="cp932")



if __name__ == "__main__":

    main()
