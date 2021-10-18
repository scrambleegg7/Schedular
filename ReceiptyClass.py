import numpy as np
import pandas as pd
import os
from datetime import date as dt

import seaborn as sns

import matplotlib.pyplot as plt

import string
import hashlib

import csv
import codecs
import glob

from WarekiClass import WarekiClass



class ReceiptyClass(object):

    def __init__(self,data_dir, base_master_dir = "L:/RECEPTY"):

        self.data_dir = data_dir
        self.base_master_dir = base_master_dir
        self.readDrugMaster()
        self.readHoliday()

        self.wareki = WarekiClass()

    def readHoliday(self):
        holiday_csv = os.path.join( self.base_master_dir, "holiday.csv" )
        #holiday_csv = "/RECEPTY/holiday.csv"
        print("[ReceiptyClass] holiday file:",holiday_csv)
        self.df_holiday = pd.read_csv(holiday_csv,names=["hdate"],parse_dates=["hdate"])

    def readDrugMaster(self):

        y_csv = os.path.join( self.base_master_dir, "y.csv" )
        #y_csv = "/RECEPTY/y.csv"
        print("[ReceiptyClass] medicine master file:", y_csv)
        #self.df_y = pd.read_csv(y_csv,header=None,usecols=[2,4,31])
        self.df_y = pd.read_csv(y_csv,encoding="Shift_JISx0213",header=None,usecols=[2,4,31])

        self.df_y.columns = ["densan_code","drugname","YJcode"]


    def weekDaySearch(self, date):

        # 1. incoming date is numpy datetime
        # 2. need to convert pandas date format
        # 3. it can be used as date object of pandas
        #
        date = pd.to_datetime(date)
        
        while True:
            
            mask = (date.strftime('%a') != "Sun") & (date not in list(self.df_holiday.hdate) )
            if mask: # and not date.isin(df_holiday.hdate):
                break
            else:
                date = date + pd.to_timedelta(1,'d')
            
        return date.to_pydatetime()

    def readReceipty(self, filename):


        print("[ReceiptyClass] reading file....", filename)
        #
        # col_names is necessary field to get variable length columns data for making data frame.
        #

        with codecs.open(filename, "r", "Shift-JIS", "ignore") as file:
            col_names = [ 'c{0:02d}'.format(i) for i in range(180) ]

            df_re = pd.read_csv(file, delimiter=",", names=col_names, engine="python" )
            #print(df_re.head())
            print("[ReceiptyClass] recepty csv data file size --> ",df_re.shape)

        #col_names = [ 'c{0:02d}'.format(i) for i in range(180) ]
        #df_re = pd.read_csv(filename,encoding="cp932", names=col_names)

        #df_re["c02"] = df_re["c02"].astype(int)

        # First of all, productname is replaced with df_y master.
        # based on densan_code

        # 1. pick up selected fields from IY record
        IY_RECORD = df_re[df_re["c00"] == "IY"][["c00","c01","c02","c03"]].copy()
        print("** BEFORE ",IY_RECORD.head(15))
        print(IY_RECORD.shape)

        # 2. rename columns name
        IY_RECORD.columns = ["ID","DUMMY","densan_code","amount"]
        # 3. change type to int32
        IY_RECORD["densan_code"] = IY_RECORD["densan_code"].astype(np.int32)
        # 4. merge
        df_iy_merge = pd.merge(IY_RECORD,self.df_y, how="left", left_on = 'densan_code', right_on = 'densan_code')


        # 5. rename columns name to original ones. eg. c00 c01 c02 ....
        col_names = [ 'c{0:02d}'.format(i) for i in range(df_iy_merge.shape[1]) ]
        df_iy_merge.columns = col_names
        # 6. check if any data has N/A drug name
        #df_merge["drugname"].isnull().sum()
        print("** BEFORE ",df_iy_merge.head(15))
        print("** SHAPE BEFORE ",df_iy_merge.shape)
        
        # 7. then reindex with original index from df_re data frame
        df_iy_merge.index = df_re[df_re["c00"] == "IY"].index

        # 8. Move merged data into original df_re master data based on original index.
        # iloc[:,:4] means that it needs to provide original df_iy_image data shape fitting into the destination data shape
        df_re[df_re["c00"] == "IY"].iloc[:,:4] = df_iy_merge
        print("** AFTER ",df_iy_merge.head(15))

        #print("**" * 20)
        #print("df_iy_merge")
        #print(df_iy_merge.head(3))
        #print(df_iy_merge.tail(3))

        return df_re
    
    def receiptyFlatten(self):

        personal_block_components = self.buildPersonalBlock("tokyo")
        df_tok = self.buildMedicineBlock(personal_block_components)
        
        personal_block_components = self.buildPersonalBlock("national")
        df_national = self.buildMedicineBlock(personal_block_components)

        df_tok =df_tok.append(df_national)

        print("[ReceiptyClass] df_tok shape ",df_tok.shape )
        
        #df_merge =  pd.merge(df_tok,self.df_out, on = ["czDate", "total_amount", "medicine"], how="left" )
        df_merge = df_tok.copy()


        print("**" * 20)
        print("")
        #print(df_merge.head(3))
        #print(df_merge.tail(3))
        

        cols = ["name","mark","hospital","medicine","nextDate","total_amount","mark2","exp","mark2","czDate","YJCode"]
        df_merge = df_merge[cols]
        newcolname = ["name","mark","hospital","medicine","nextDate","total_amount","mark2","exp","standard","czDate","YJCode"]
        df_merge.columns = newcolname

        #print("[ReceiptyClass] df_merge shape ", df_merge.shape)
        #print(df_merge.head() )
        #print("**" * 20)

        #print("* minimum czDate", min(df_merge.czDate))

        integrate_csvfile = os.path.join(self.data_dir, "integrate.csv")
        df_merge.to_csv(integrate_csvfile, index=False,encoding="cp932")

        return df_merge

    def buildPersonalBlock(self,dest="tokyo"):

        RECEPTY_DIR = os.path.join( self.data_dir,   dest )
        filename = os.path.join(RECEPTY_DIR,"RECEIPTY.CYO")
        #filename = self.data_dir + "/" + dest + "/RECEIPTY.CYO"
        
        df_recepty = self.readReceipty(filename)
        RE_index = self.getREindex(df_recepty)
        #
        # build personal block 
        # baed on RE_index 
        #
        personal_block_components = []
        for idx, re_idx in enumerate(RE_index[:-1] ):
            first_index = re_idx
            last_index = RE_index[idx+1]
            
            personal_block = df_recepty[first_index:last_index]
            personal_block_components.append(personal_block)

        return personal_block_components

    def buildMedicineBlock(self,personal_block_components):
        #
        # build personal block to have shoho / chozai / medicine detail data.
        # per person
        #
        #personal_medicine_details = []
        #for personal_block in personal_block_components:
        for idx, personal_block in enumerate( personal_block_components ):

            RE_mask = personal_block["c00"] == "RE"
            RE_personal = personal_block[RE_mask].copy()
            
            df_medicine_details = self.getMedicineData(RE_personal,personal_block)

            #if df_medicine_details == None:
            #    continue  

            if idx == 0:
                df_personal = df_medicine_details.copy()
            else:
                df_personal = df_personal.append( df_medicine_details )


        df_personal.reset_index(drop=True)
        return df_personal

    def getMedicineData(self,RE_personal, personal_block):

        SH_index = self.getSHCZIY(personal_block)

        wareki_ops = lambda d:self.wareki.getDate(d)
        seireki_ops = lambda d:self.wareki.getDateSeireki(d)
        

        personal_data = personal_block.copy()
        personal_data.reset_index(drop=True)

        name = RE_personal["c04"].values[0]
        personal_id = RE_personal["c01"].values[0]
        hospital = RE_personal["c12"].values[0]

        personal_medicine_block = []

        for idx, sh_idx in enumerate(SH_index[:-1] ):
            
            # for debug, confirm first/last index each shoho data.
            #print(idx,sh_idx)
            first_index = sh_idx
            last_index = SH_index[idx+1]
            
            medicine_block = personal_data[first_index:last_index]
            #print(medicine_block.shape[0])
            
            personal_medicine_block.append(medicine_block)


        nextDateDict = {}
        medicine_details = []
        for p in personal_medicine_block:

            # shoho 
            sh_mask = p["c00"] == "SH"
            SH = p[sh_mask].copy()
            zaikei = SH["c02"].values[0]                 
            
            cz_mask = p["c00"] == "CZ"
            CZ = p[cz_mask].copy()
            CZ[ ['c05'] ] = CZ[ ['c05'] ].astype(int)
            
            days = pd.to_timedelta(CZ.loc[:,"c05"],'d')
            days_14 = pd.to_timedelta(14,'d')
            CZ.c02 = CZ.c02.apply(seireki_ops)
            CZ.c03 = CZ.c03.apply(seireki_ops)
            
            #print("cz03", CZ.c03, days)
            
            CZ.c06 = ( CZ.c03 + days )
            
            #days = timedelta(days=)
            for cz_i in range( CZ.shape[0] ):
                
                #days = timedelta(days=)
                shoDate = CZ["c02"].values[cz_i]
                czDate = CZ["c03"].values[cz_i]
                czcount = CZ["c04"].values[cz_i]
                czdays = CZ["c05"].values[cz_i]
                nextDate = CZ["c06"].values[cz_i]
                
                # check holiday and weekday
                nextDate = self.weekDaySearch(nextDate) 
                expDate = (nextDate + days_14)

                if zaikei in ["1","3"]:
                    if not czDate in nextDateDict:
                        nextDateDict[czDate] = nextDate
                else:
                    if czDate in nextDateDict:
                        nextDate = nextDateDict[czDate]

                iy_mask = p["c00"] == "IY"
                IY = p[iy_mask].copy()
                #print(IY.shape[0])
                IY[ ['c03'] ] = IY[ ['c03'] ].astype(float)


                for iy_i in range( IY.shape[0] ):
                    amount = IY["c03"].values[iy_i]
                    drugName = IY["c04"].values[iy_i]

                    #YJCode
                    YJCode = IY["c05"].values[iy_i]


                    if zaikei in ["1","3"]:
                        total_amount = float(czdays * amount)
                    else:
                        total_amount = amount

                    drug_detail = [name,'',hospital,drugName,nextDate,total_amount,'',expDate,'',czDate, YJCode]
                    medicine_details.append(drug_detail )

        cols = ["name","mark","hospital","medicine","nextDate","total_amount","mark2","exp","mark3","czDate","YJCode"]
        df_medicine = pd.DataFrame( medicine_details, columns=cols )

        return df_medicine


    #
    # first we pick up RE flag from RECEIPTY.CIO file
    #
    def getREindex(self, df_re):

        mask = df_re["c00"] == "RE"
        row_length = df_re.shape[0]
        row_range = np.array( range(row_length) )
        RE_index = row_range[mask]
        RE_index = np.append( RE_index, row_length  )
        
        return RE_index

    def getSHCZIY(self, pblock):

        mask = pblock["c00"] == "SH"
        row_length = pblock.shape[0]
        row_range = np.array( range(row_length) )
        SH_index = row_range[mask]

        personal_block_last_index = len(pblock)
        SH_index = np.append(SH_index, personal_block_last_index )

        return SH_index 