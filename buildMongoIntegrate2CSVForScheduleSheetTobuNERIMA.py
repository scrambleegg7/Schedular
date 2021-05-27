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


from pymongo import MongoClient
from mongo_connect import ReceptyMongo
from bson.decimal128 import Decimal128

from MyDateClass import MyDateClass
from StockMasterClass import StockMasterClass


def connectMongDB():

    mongoObj = ReceptyMongo()
    mongoObj.setSchedularTable()

    return mongoObj



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

    #table = connectDynamoDBTable()
    mongoObj = connectMongDB()


    #data_dir = "/Users/donchan/Documents/myData/miyuki"
    if platform.system() == "Windows":
        recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/RECEPTY"
    
    receptyCls = ReceiptyClass(recepty_data_dir)

    df_merge = receptyCls.receiptyFlatten()
    print("[buildMongoIntegrate2CSVForScheduleSheet] * shape of tokyo/national integrated date", df_merge.shape)
    print("[buildMongoIntegrate2CSVForScheduleSheet] icluding Tobunerima Clinic")
    

    # changed on Apr.3rd 
    # 東武練馬クリニック is renamed to exclude prefix 北桜
    #mask = df_merge.hospital.str.contains('北桜')
    # mask = df_merge.hospital.str.contains('東武練馬クリニック')

    #df_merge = df_merge[mask].sort_values(["nextDate","name"])
    df_merge = df_merge.sort_values(["medicine","hospital","nextDate"])
    # #df_merge = df_merge[~mask].sort_values(["nextDate","name"])

    print("[buildMongoIntegrate2CSVForScheduleSheet] * shape of everything including ....", df_merge.shape)

    #tKey = nextDate + czDate + name + hospital + medicine + str_total_amount
    merge_csvfile = os.path.join(data_dir, "merge.csv")

    #
    # set Today with MyDateClass
    #
    myDateObj = MyDateClass()
    YYYYMMDD = myDateObj.strYYYYMMDD()
    YYYY = myDateObj.strYYYY()

    mask = df_merge.nextDate >= pd.to_datetime( myDateObj.TDY )
    df_merge = df_merge[mask].copy()
    print("[+] new size of df_merge", df_merge.shape)
    df_merge.nextDate = df_merge.nextDate.dt.strftime("%Y/%m/%d")
    df_merge.czDate = df_merge.czDate.dt.strftime("%Y/%m/%d")
    df_merge.exp = df_merge.exp.dt.strftime("%Y/%m/%d")     
    df_merge.total_amount = df_merge.total_amount.apply( lambda x:'{:.2f}'.format(x) ) 
    #print(df_merge.head(3))
    #print(df_merge.tail(3))

    mainDir = u"\\\\EMSCR01\\ReceptyN\\TEXT\\"
    #udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\在庫一覧%s*.CSV" % (YYYYMMDD)
    stocklist =u"在庫一覧%s*.CSV" % (YYYYMMDD)
    udirPath = os.path.join( mainDir, stocklist  )
    stockCls = StockMasterClass(udirPath,test=True)
    df_stock = stockCls.getStock()


    print("")
    print(df_stock.head(3))
    print(df_stock.tail(3))
    df_merge = df_merge.reset_index()
    
    
    yj_uniq = df_merge.YJCode.unique()
    for i, target_yj in enumerate( yj_uniq ):

        ## Selecting with target yj code
        mask = df_merge.YJCode == target_yj
        df_merge_sel = df_merge[mask]
        index_list = list( df_merge_sel.index )

        print("[+] target YJ code")
        print(target_yj)
        
        print("[+] index list")
        print(index_list)
        print("[+] total amount by index")
        print(df_merge_sel.total_amount.values)
        print("[+] details......")
        print( df_merge.loc[index_list,:] )

        st_mask = df_stock.yjcode == target_yj
        df_stock_sel = df_stock[st_mask]
        if df_stock_sel.shape[0] == 0:
            print("[-] Cannot get currnet stock")
            print("[-] not found target YJcode : " ,target_yj)
            drugname = df_merge.loc[index_list,"medicine"].values[0]
            print("[+] Start to search with drugname from df_merge : " ,drugname)
            st_mask = df_stock.drugname == drugname
            df_stock_sel = df_stock[st_mask]

        print("[+] selected df_stock_sel ......")
        print( df_stock_sel )

        new_stock = df_stock_sel.stock.values
        print("current stock : ", new_stock)
        if len(new_stock) > 1:
            print("[-] stock data has more than 1.")
        
            try:
                ptp_mask = df_stock_sel.standard.str.contains('ＰＴＰ')
                ptp_stock = df_stock_sel[ptp_mask].stock.values[0]
                ptp_standard = df_stock_sel[ptp_mask].standard.values[0]

                non_ptp_stock = df_stock_sel[~ptp_mask].stock.values[0]
                non_ptp_standard = df_stock_sel[~ptp_mask].standard.values[0]

            except:
                print("[-] might not be PTP or tablets................. ")
                ptp_stock = max( new_stock )
                ptp_standard = df_stock_sel[df_stock_sel.stock == ptp_stock].stock.values[0]
        else:
            if df_stock_sel.shape[0] == 0:
                ptp_stock = 0    
                ptp_standard = ""
            else:
                ptp_stock = df_stock_sel.stock.values[0]
                ptp_standard = df_stock_sel.standard.values[0]



        print("[+] ptp stock : ", ptp_stock)
        print("[+] ptp standard : ", ptp_standard)
        #try:    
        new_stocks = []
        #df_merge_sel.total_amount.values
        tobu_mask = df_merge_sel.hospital.str.contains('東武練馬クリニック').tolist()
        print("[+] tobunerima masking by df_merge_selection . ", tobu_mask)
        amounts = df_merge_sel.total_amount.values

        for cnt, idx in enumerate( index_list ):
            

            print("cnt: %d , idx: %d " % (cnt,idx) )
            print("amount : ", amounts[cnt] )

            if tobu_mask[cnt] and len(new_stock) > 1:
                non_ptp_stock = non_ptp_stock - float( amounts[cnt] )
                mask2_var = non_ptp_stock
                standard_var = non_ptp_standard
            else:
                ptp_stock = ptp_stock - float( amounts[cnt] )
                mask2_var = ptp_stock
                standard_var = ptp_standard
                        
            #print(df_merge_yj_amt[cnt])
            #new_stock  new_stock - df_merge_yj_amt[cnt]
            #new_stocks.append (new_stock)

            #print(df_merge.loc[idx,"total_amount"].values)
            df_merge.loc[idx,"mark2"] = mask2_var
            df_merge.loc[idx,"standard"] = standard_var
                 

        #except:
        #    print("Stock Calculation Loop Error, YJCode error", target_yj)
        #    print("")
            

    df_merge.to_csv(merge_csvfile, index=False, encoding="cp932")
        
    """
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


        query={'$and':[{'name':name},{'czDate':czDate}, {'medicine':medicine}, {'nextDate':nextDate}, { 'total_amount': Decimal128(str_total_amount)} ]}
        ret_obj = mongoObj.findAndLimit(query)
        
        if ret_obj == dict():
            if i % 200 == 0:
                print("[buildMongoIntegrate2CSVForScheduleSheet] --> MongoDB Not found", row_data)
            
            df_Select.loc[i] = row_data
            continue
        
        ret = ret_obj[0]
        returned_items = ret.values()
        


            

    #data_dir = "/Volumes/myShare/ipython"
    integrate_csvfile = os.path.join(data_dir, "integrate2TobuNerima.csv")

    print("[buildMongoIntegrate2CSVForScheduleSheet] Selected data having new czDate / name. ",df_Select.shape)
    print("[buildMongoIntegrate2CSVForScheduleSheet] data is saved.", integrate_csvfile)
    df_Select.to_csv(integrate_csvfile, index=False, encoding="cp932")
    """


if __name__ == "__main__":

    main()
