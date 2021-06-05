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


class SchedulerClass(object):
    
    def __init__(self,test=False):
        
        super(SchedulerClass,self).__init__()
        
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

    def connectMongDB(self):

        mongoObj = ReceptyMongo()
        mongoObj.setSchedularTable()

        return mongoObj

    def readFiles(self):


        #READ OLD FILES
        myOLDPATH = os.path.join(self.recepty_data_dir,"old")
        myDirs = []
        for (dirpath, dirnames, filenames) in walk(myOLDPATH):
            myDirs.extend(dirnames)
            break

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


    def generateMergeFile(self):



        def h2zConv(x):
            return jaconv.h2z( x, digit=True, ascii=True)

                            
        mongoObj = self.connectMongDB()

        df_old_merges = self.readFiles()
        receptyCls = ReceiptyClass(self.recepty_data_dir)

        df_merge = receptyCls.receiptyFlatten()

        if df_old_merges.shape[0] > 0:
            print("[+] df_old_merges is existed. and merge with currnet data. ")
            df_merge = pd.concat( [df_merge, df_old_merges] )   
        
        print("[buildMongoIntegrate2CSVForScheduleSheet] * shape of tokyo/national integrated date", df_merge.shape)
        print("[buildMongoIntegrate2CSVForScheduleSheet] icluding Tobunerima Clinic")
        

        #df_merge = df_merge.sort_values(["medicine","hospital","nextDate"])
        df_merge = df_merge.sort_values(["medicine","nextDate"])
    
        print("[buildMongoIntegrate2CSVForScheduleSheet] * shape of everything including ....", df_merge.shape)

        #tKey = nextDate + czDate + name + hospital + medicine + str_total_amount
        merge_csvfile = os.path.join(self.data_dir, "merge.csv")

        #
        # set Today with MyDateClass
        #
        #YYYY = myDateObj.strYYYY()
        mask = df_merge.nextDate >= pd.to_datetime( self.TDY )
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
        stocklist =u"在庫一覧%s*.CSV" % (self.YYYYMMDD)
        udirPath = os.path.join( mainDir, stocklist  )
        stockCls = StockMasterClass(udirPath,test=True)
        df_stock = stockCls.getStock()
        df_stock.drugname = df_stock.drugname.apply( h2zConv )

        if self.test:
            print("")
            print(df_stock.head(3))
            print(df_stock.tail(3))
        
        df_merge = df_merge.reset_index(drop=True)
        
        
        yj_uniq = df_merge.YJCode.unique()
        for i, target_yj in enumerate( yj_uniq ):

            ## Selecting with target yj code
            mask = df_merge.YJCode == target_yj
            df_merge_sel = df_merge[mask]
            index_list = list( df_merge_sel.index )

            #if target_yj == "3399004M4017": # イコサペント酸エチル
            #    self.test = True
            #else:
            #    self.test = False

            #
            # clear values with default 
            #
            ptp_mask = None
            ptp_stock = None
            ptp_standard = None
            non_ptp_stock = None
            non_ptp_standard = None


            if self.test:
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
                
                drugname = df_merge.loc[index_list,"medicine"].values[0]
                st_mask = df_stock.drugname == drugname
                df_stock_sel = df_stock[st_mask]

                if df_stock_sel.shape[0] == 0:
                    # change hankaku
                    #drugname = jaconv.z2h( drugname, digit=True, ascii=True) 
                    #drugname  = df_merge.loc[index_list,"medicine"].apply( jaconv. )
                    #print("[+] hankaku", drugname)
                    #st_mask = df_stock.drugname == drugname
                    st_mask = df_stock.drugname.str.contains( drugname )
                    df_stock_sel = df_stock[st_mask]

                if self.test:
                    print("[-] Cannot get currnet stock")
                    print("[-] not found target YJcode : " ,target_yj)                    
                    print("[+] Start to search with drugname from df_merge : " ,drugname)
                

            new_stock = df_stock_sel.stock.values
            
            if self.test:
                print("[+] selected df_stock_sel ......")
                print( df_stock_sel )
                print("current stock : ", new_stock)

            if len(new_stock) > 1:
                
                
                if self.test:
                    print("[-] stock data has more than 1.")
            
                try:
                    ptp_mask = df_stock_sel.standard.str.contains('ＰＴＰ')
                    ptp_stock = df_stock_sel[ptp_mask].stock.values[0]
                    ptp_standard = df_stock_sel[ptp_mask].standard.values[0]

                    non_ptp_stock = df_stock_sel[~ptp_mask].stock.values[0]
                    non_ptp_standard = df_stock_sel[~ptp_mask].standard.values[0]

                except:
                    if self.test:
                        print("[-] might not be PTP or tablets................. ")
                    ptp_stock = max( new_stock )
                    ptp_standard = df_stock_sel[df_stock_sel.stock == ptp_stock].standard.values[0]
            else:
                if df_stock_sel.shape[0] == 0:
                    ptp_stock = 0    
                    ptp_standard = ""
                else:
                    ptp_stock = df_stock_sel.stock.values[0]
                    ptp_standard = df_stock_sel.standard.values[0]


            if self.test:
                print("[+] ptp stock : ", ptp_stock)
                print("[+] ptp standard : ", ptp_standard)
            #try:    
            new_stocks = []
            #df_merge_sel.total_amount.values
            tobu_mask = df_merge_sel.hospital.str.contains('東武練馬クリニック').tolist()
            if self.test:
                print("[+] tobunerima masking by df_merge_selection . ", tobu_mask)
            amounts = df_merge_sel.total_amount.values

            for cnt, idx in enumerate( index_list ):
                
                if self.test:
                    print("cnt: %d , idx: %d " % (cnt,idx) )
                    print("amount : ", amounts[cnt] )

                if tobu_mask[cnt] and len(new_stock) > 1:
                    try:
                        non_ptp_stock = non_ptp_stock - float( amounts[cnt] )
                        mask2_var = non_ptp_stock
                        standard_var = non_ptp_standard
                    except:
                        ptp_stock = ptp_stock - float( amounts[cnt] )
                        mask2_var = ptp_stock
                        standard_var = ptp_standard

                    if self.test:
                        print("[+] tobunerima stock ")
                        print("[+] mask2_var", mask2_var)
                        print("[+] standard", standard_var)
                        

                #elif tobu_mask[cnt] and len(new_stock) == 1:
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

        self.df_merge = df_merge.copy()
        print("[+] df_merge final shape %s" % (df_merge.shape,))     


    def readInputData(self):

        indata =u"入庫*.CSV" 
        incomingDir = os.path.join(self.recepty_data_dir,"incoming",indata)
        inputDataClass = InputDataClass(incomingDir)

        df_in = inputDataClass.getDfMasterData()

        return df_in

    def BuildOrderData(self, df_merge=None):

        if df_merge == None:
            df_merge = self.df_merge

        df_inputdata = self.readInputData()

        print("-- " * 20)
        print("[+] input data shape")
        print(df_inputdata.shape)
        #print(df_inputdata.head(3))
        #print(df_inputdata.tail(3))
        print("")

        df_merge.nextDate = pd.to_datetime( df_merge.nextDate )
        fwd14d = pd.to_datetime( self.TDY ) + pd.Timedelta(10,unit="D")
        self.df_merge = df_merge[ (df_merge.nextDate <= fwd14d) & (df_merge.mark2 < 0) ].copy()
        #self.df_merge = df_merge.reset_index(drop=True)        
        print("[+] df_merge shape %s upto %s " % (self.df_merge.shape, fwd14d)   )

        #print(self.df_merge.head(3))
        #print(self.df_merge.tail(3))

        df_res = self.consolidateByYJCode()
        self.buildOrderEPICSV(df_consoli_by_YJcode=df_res, df_in=df_inputdata)

    def consolidateByYJCode(self):

        # target negative mark2 
        df_merge_ = self.df_merge[self.df_merge.mark2 < 0].copy()      
        df_consoli_by_YJcode = df_merge_.groupby(["medicine","YJCode","standard"])["mark2"].sum().reset_index()
        print("-- " * 20)
        print("[+] consolidated by YJ/standard")
        print("[+] shape of consolidated :  %s "  % ( df_consoli_by_YJcode.shape, ) )
        #print(df_consoli_by_YJcode.head(3))
        #print(df_consoli_by_YJcode.tail(3))
        print("")

        return df_consoli_by_YJcode

    def buildOrderEPICSV(self, df_consoli_by_YJcode, df_in):

        df_consoli_by_YJcode.mark2 = df_consoli_by_YJcode.mark2 * -1

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
                df_in_sel = df_in_sel[ df_in_sel.indate == max_indate ]
                print("[+] from multiple drugname (matched with YJcode) : ", df_in_sel.drugname, df_in_sel.standard, df_in_sel.indate, yjcode, df_in_sel.jancode) 
                print( df_in_sel )
            elif df_in_sel.shape[0] == 0:

                print("missing medecine name with yjcode", yjcode, medicine, standard)
                mask = (df_in.drugname == medicine) & ( df_in.standard.str.contains( standard ) )
                df_in_sel = df_in[mask]
                if df_in_sel.shape[0] == 0:
                    print( medicine,yjcode,standard,mark2  )
                    raise("error" )
                elif df_in_sel.shape[0] > 1:
                    max_indate = df_in_sel.indate.max()
                    df_in_sel = df_in_sel[ df_in_sel.indate == max_indate ].iloc[0]
                    print("[+] from multiple drugname : ",df_in_sel.drugname, df_in_sel.standard, df_in_sel.indate) 
                else:
                    pass 
                    print("[+] from drugname : ", df_in_sel.drugname.iloc[0], df_in_sel.standard.iloc[0])    


            #print(df_in_sel)

            if int( df_in_sel.box )  <= 0:
                print( medicine,yjcode,standard, df_in_sel.box  )
                print("BOX error:"  )
                df_in_sel.box = 1.0
                

            try:
                minimumUnit = float( df_in_sel.num ) / float( df_in_sel.box )
            except ZeroDivisionError:
                print("divid zero")
                minimumUnit = 0

            try:
                orderUnit = round( mark2 / minimumUnit + 0.5 )
            except ZeroDivisionError:
                print("divid zero with nminimum unit.")
            
            sellar =   df_in_sel.wholesaler

            #try:
            if int( df_in_sel.jancode ) > 0 and df_in_sel.jancode != np.nan:
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


    schedularClass = SchedulerClass()
    schedularClass.generateMergeFile()
    #schedularClass.BuildOrderData()



if __name__ == "__main__":

    main()
