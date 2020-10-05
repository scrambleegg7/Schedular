import numpy as np
import pandas as pd
import os
from datetime import date as dt

import seaborn as sns
from time import time
import matplotlib.pyplot as plt

import string
import hashlib

import csv
import codecs
import boto3
from decimal import *

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from pymongo import MongoClient
from mongo_connect import ReceptyMongo

from bson.decimal128 import Decimal128


import platform

def loadCurrentData(data_dir):

    date_ops = lambda x:pd.datetime.strptime(x,"%Y/%m/%d")

    #filename = os.path.join("/Volumes/myShare/ipython","longterm2.csv")

    filename = os.path.join(data_dir,"longterm2.csv")

    print("[insertLGSchedularBatch] filename for longterm2-->",filename)
    print("- " * 40 )
    print("[insertLGSchedularBatch] longterm2 dataframe : ")
    
    #df_lg = pd.read_csv(filename,encoding="Shift_JISx0213")
    df_lg = pd.read_csv(filename , encoding="Shift_JISx0213" )

    print( df_lg.tail() )    

    col_names = [ 'c{0:02d}'.format(i) for i in range( len( df_lg.columns ) ) ]

    df_lg.columns = col_names

    df_lg.c04 = df_lg.c04.apply(date_ops)
    df_lg.c07 = df_lg.c07.apply(date_ops)
    df_lg.c09 = df_lg.c09.apply(date_ops)

    # set 2 decimal points 
    df_lg.c05 = df_lg.c05.apply( lambda x:'{:.2f}'.format(x) )
    

    df_lg.c01 = df_lg.c01.fillna("")
    df_lg.c06 = df_lg.c06.fillna("")
    df_lg.c08 = df_lg.c08.fillna("")
        
    mask = df_lg.c09.notnull()

    uniq_date = sorted( df_lg.c09[mask].unique() )

    prv2_date = uniq_date[-1]
    min_prv_date = min( df_lg.c09[mask]  )
    max_prv_date = max( df_lg.c09[mask]  )
    df_lg.c09 = df_lg.c09.fillna( min_prv_date )

    cols = ["name","mark","hospital","medicine","nextDate","total_amount","mark2","exp","standard","czDate"]
    df_lg.columns = cols

    print("[insertLGSchedularBatch] max date (previous date of latest date)  ..", prv2_date)
    return df_lg, prv2_date

def connectDynamoDBTable():

    dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
    table = dynamodb.Table('schedular')
    #print("TABLE creation date:",table.creation_date_time) 

    return table

def connectMongDB():

    mongoObj = ReceptyMongo()
    mongoObj.setSchedularTable()

    return mongoObj


def saveMongoDB(data_dir):

    #table = connectDynamoDBTable()

    mongoObj = connectMongDB()

    ret = mongoObj.delete_many()
    print("[insertLGSchedularBatch]  counting schedular table -->   %s " % ret  ) 

    num_data = mongoObj.count()
    print("[insertLGSchedularBatch]  counting schedular table -->   %d " % num_data  ) 

    #df_merge = receptyCls.receiptyFlatten()
    #print("* shape of tokyo/national integrated date", df_merge.shape)
    df_merge , prv2_date = loadCurrentData(data_dir)
    print("[insertLGSchedularBatch] csv longterm total count:", len(df_merge))
    print("[insertLGSchedularBatch] Latest czDate of csv longterm", prv2_date)
    
    h,w = df_merge.shape

    step50 = range(0,h,50)

    batch_start_time = time()

    for k in step50:
        #print(i,i+50)
        buffer = []
        for i in range(k,k+50):
            if i > (h-1):
                break

            ser = df_merge.iloc[i,:]
            name = ser[0]
            hospital = ser[2]
            medicine = ser[3]
            nextDate = ser[4].strftime("%Y/%m/%d")
            total_amount = ser[5]
            exp = ser[7].strftime("%Y/%m/%d")
            czDate = ser[9].strftime("%Y/%m/%d")

            str_total_amount = str(ser[5])

            # for debugging use ...
            #print( name,czDate,medicine,str_total_amount  )

            #dec_total_amount = Decimal(str_total_amount)
            dec_total_amount = Decimal128(str_total_amount)
            

            try:
                tKey = czDate + name + hospital + medicine + str_total_amount
            except:
                print("- " * 40 )
                print("[insertLGSchedularBatch] -  tkey generating Error ....")

                print("[insertLGSchedularBatch] name,hopital,medicne,czDate,total_amount")
                print(name,hospital,medicine,czDate,total_amount )
                print("")
                continue

            if i != 0 and i % 200 == 0:
                print("[insertLGSchedularBatch] inserted counter --> ",i)
                print(nextDate,name,hospital,medicine,dec_total_amount,exp,czDate)

            schedule1 = {
                #"tableKey": tKey,
                "nextDate": nextDate,
                "name": name,
                "hospital": hospital,
                "medicine": medicine,
                "total_amount":dec_total_amount,
                "expiry":exp,
                "czDate":czDate
            }

            buffer.append(schedule1)

        #print(buffer)

        ret = mongoObj.add_many(buffer)
        print("[insertLGSchedularBatch]")
        #print(ret)

    
    print("[insertLGSchedularBatch] Table main key : nextDate + czDate + name + hospital + medicine + str_total_amount")
    print("[insertLGSchedularBatch] Batch consuming %.4fs  "% (time() - batch_start_time) )
    #print("total data records from RECEPTY: ", i)

    num_data = mongoObj.count()
    print("[insertLGSchedularBatch] after Inserting, counting schedular table -->   %d " % num_data  ) 

    #table = connectDynamoDBTable()
    #print("[insertLGSchedularBatch] total table count after insertion.", table.item_count)

def main():

    if platform.system() == "Windows":
        #recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/ipython"

    saveMongoDB(data_dir)

if __name__ == "__main__":

    main()
