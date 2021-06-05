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

import mojimoji
import jaconv


class OrderEPIClass(object):
    
    def __init__(self,test=False):
        
        super(OrderEPIClass,self).__init__()
        
        self.tobuclinic =  u"東武練馬ｸﾘﾆｯｸ"
        #self.packmachine = u"分包機-1-"
        self.packmachine = u"分包機"

        self.df_merge = None

        self.test = test


        #data_dir = "/Users/donchan/Documents/myData/miyuki"
        if platform.system() == "Windows":
            self.recepty_data_dir = "L:/RECEPTY"
            self.data_dir = "L:/ipython"
        else:
            self.data_dir = "/Volumes/myShare/RECEPTY"

        self.getDate()

    def getDate(self):

        myDateObj = MyDateClass()
        self.YYYYMMDD = myDateObj.strYYYYMMDD()
        self.TDY = myDateObj.TDY

    def readMERGEFile(self):
        filename = os.path.join(self.data_dir,"merge.csv")

        with codecs.open(filename, "r", "Shift-JIS", "ignore") as file:
            df_merge = pd.read_csv(file)

        print("** "*20)
        print("[+] df_merge shape ")
        print(df_merge.shape)
        #display( df_merge.head(3))
        #display( df_merge.tail(3))
        return df_merge.copy()

    def readInputData(self):

        indata =u"入庫*.CSV" 
        incomingDir = os.path.join(self.recepty_data_dir,"incoming",indata)
        inputDataClass = InputDataClass(incomingDir)

        df_in = inputDataClass.getDfMasterData()

        return df_in.copy()



    def buildOrderEPICSV(self):

        df_merge = self.readMERGEFile()
        df_in = self.readInputData()

        print(df_in.info())

        print(  df_in.jancode[:10 ])
        # target negative mark2 
        #df_merge_ = df_merge[df_merge.mark2 < 0].copy()


        df_merge.nextDate = pd.to_datetime( df_merge.nextDate )
        #fwd14d = pd.to_datetime( "2021/6/3" ) + pd.Timedelta(10,unit="D")
        #df_merge_ = df_merge[ (df_merge.nextDate <= fwd14d) & (df_merge.mark2 < 0) ].copy()
        fwd14d = pd.to_datetime( self.TDY ) + pd.Timedelta(10,unit="D")
        df_merge = df_merge[ (df_merge.nextDate <= fwd14d) & (df_merge.mark2 < 0) ].copy()

        df_consoli_by_YJcode = df_merge.groupby(["medicine","YJCode","standard"])["mark2"].sum()
        df_consoli_by_YJcode= df_consoli_by_YJcode.reset_index()
        df_consoli_by_YJcode.mark2 = df_consoli_by_YJcode.mark2 * -1

        #display( df_consoli_by_YJcode.head(3))
        #display( df_consoli_by_YJcode.tail(3))
        orderepis = []
        non_orderepis = []

        for idx in list( df_consoli_by_YJcode.index ):

            medicine = df_consoli_by_YJcode.loc[idx,"medicine"]

            yjcode = df_consoli_by_YJcode.loc[idx,"YJCode"]
            standard = df_consoli_by_YJcode.loc[idx,"standard"]
            mark2 = df_consoli_by_YJcode.loc[idx,"mark2"]

            mask = ( df_in.yjcode == yjcode ) & ( df_in.standard.str.contains( standard ) )
            df_in_sel = df_in[mask]
            
            
            print("-- " * 20)
            #mask = df_in_sel.standard.str.contains( standard )
            #df_in_sel = df_in_sel[mask]

            if df_in_sel.shape[0] > 0:
                max_indate = df_in_sel.indate.max()
                df_in_sel = df_in_sel[ df_in_sel.indate == max_indate ].iloc[0]
                #print("[+] from multiple drugname (matched with YJcode) : ",df_in_sel.drugname, df_in_sel.standard, df_in_sel.indate, yjcode) 

            elif df_in_sel.shape[0] == 0:

                #print("missing medecine name", yjcode, medicine, standard)
                mask = (df_in.drugname == medicine) & ( df_in.standard.str.contains( standard ) )
                df_in_sel = df_in[mask]
                if df_in_sel.shape[0] == 0:
                    print( medicine,yjcode,standard,mark2  )
                    raise("error" )
                elif df_in_sel.shape[0] > 1:
                    max_indate = df_in_sel.indate.max()
                    df_in_sel = df_in_sel[ df_in_sel.indate == max_indate ].iloc[0]
                    #print("[+] from multiple drugname : ",df_in_sel.drugname, df_in_sel.standard, df_in_sel.indate) 
                else:
                    pass 
                    #print("[+] from drugname : ", df_in_sel.drugname.iloc[0], df_in_sel.standard.iloc[0])    


            #print(df_in_sel)

            if df_in_sel.box  <= 0:
                print( medicine,yjcode,standard, df_in_sel.box  )
                print("BOX error:"  )
                df_in_sel.box = 1.0
                

            try:
                minimumUnit = df_in_sel.num / df_in_sel.box
            except ZeroDivisionError:
                print("divid zero")
                minimumUnit = 0

            try:
                orderUnit = round( mark2 / minimumUnit + 0.5 )
            except ZeroDivisionError:
                print("divid zero with nminimum unit.")
            
            sellar =   df_in_sel.wholesaler

            #try:
            if df_in_sel.jancode != np.nan and  df_in_sel.jancode != None and df_in_sel.isnull().sum() == 0:
                print(df_in_sel.jancode, medicine, sellar, minimumUnit, orderUnit, mark2  )

                orders = [df_in_sel.jancode, orderUnit, sellar]
                orderepis.append(orders )




            else:
                print("[+] not order epi : ", medicine, sellar, minimumUnit, orderUnit, mark2  ) 
                non_orders = [medicine, sellar, minimumUnit, orderUnit, mark2]
                non_orderepis.append( non_orders)

            #except:
            #    print(df_in_sel.jancode, medicine, standard)



        self.saveCSVFiles(orderepis,non_orderepis)
    
    def saveCSVFiles(self, orderepis, non_orderepis):

        data_dir = self.data_dir

        ofilename = os.path.join(data_dir,"orderepi.csv")
        nonfilename = os.path.join(data_dir,"non_epi.csv")


        df = pd.DataFrame(orderepis)
        df.to_csv(ofilename, index=False, encoding="cp932",header=False)


        df = pd.DataFrame(non_orderepis)
        df.to_csv(nonfilename, index=False, encoding="cp932",header=False)        


def main():


    orderEPIClass = OrderEPIClass()
    orderEPIClass.buildOrderEPICSV()



if __name__ == "__main__":

    main()
