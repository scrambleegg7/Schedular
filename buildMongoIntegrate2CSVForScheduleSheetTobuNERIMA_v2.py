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
from InputDataClass import InputDataClass

from glob import glob
from os import walk


def getDate():
    pass

def connectMongDB():

    mongoObj = ReceptyMongo()
    mongoObj.setSchedularTable()

    return mongoObj

def readFiles():

    #data_dir = "/Users/donchan/Documents/myData/miyuki"
    if platform.system() == "Windows":
        recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/RECEPTY"


    #READ OLD FILES
    myOLDPATH = os.path.join(recepty_data_dir,"old")

    myDirs = []
    for (dirpath, dirnames, filenames) in walk(myOLDPATH):
        myDirs.extend(dirnames)
        break

    print(myDirs)


    merges = []
    for d in myDirs:

        print("** "* 20 )
        print("[+] Target Month", d)
        old_recepty_dir = os.path.join(myOLDPATH,d)
        receptyCls = ReceiptyClass(old_recepty_dir)
        df_merge = receptyCls.receiptyFlatten()
        print("[+] old recepty shape of data  %s " % (df_merge.shape,) )
        print("")

        merges.append(df_merge)

    df_old_merges = pd.concat( merges )
    print("[+] shape of df_old_merges ",   df_old_merges.shape)

    return df_old_merges


def generateMergeFile():




    mongoObj = connectMongDB()

    df_old_merges = readFiles()

    #data_dir = "/Users/donchan/Documents/myData/miyuki"
    if platform.system() == "Windows":
        recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/RECEPTY"
    
    receptyCls = ReceiptyClass(recepty_data_dir)

    df_merge = receptyCls.receiptyFlatten()

    if df_old_merges.shape[0] > 0:
        print("[+] df_old_merges is existed. and merge with currnet data. ")
        df_merge = pd.concat( [df_merge, df_old_merges] )   
    
    print("[buildMongoIntegrate2CSVForScheduleSheet] * shape of tokyo/national integrated date", df_merge.shape)
    print("[buildMongoIntegrate2CSVForScheduleSheet] icluding Tobunerima Clinic")
    

    # changed on Apr.3rd 
    # 東武練馬クリニック is renamed to exclude prefix 北桜
    #mask = df_merge.hospital.str.contains('北桜')
    # mask = df_merge.hospital.str.contains('東武練馬クリニック')

    #df_merge = df_merge[mask].sort_values(["nextDate","name"])
    #df_merge = df_merge.sort_values(["medicine","hospital","nextDate"])
    df_merge = df_merge.sort_values(["medicine","nextDate"])
    
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

    return df_merge     


def BuildOrderData(df_merge):

    pass


def main():

    df_merge = generateMergeFile()




if __name__ == "__main__":

    main()
