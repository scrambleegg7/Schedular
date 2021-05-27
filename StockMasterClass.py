# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

import sys
import codecs
import glob

from MasterDataClass import MasterDataClass


class StockMasterClass(MasterDataClass):
    
    def __init__(self,udir,test=False):
        
        super(StockMasterClass,self).__init__(udir,test)
        
    def loadData(self):

        with codecs.open(self.first, "r", "Shift-JIS", "ignore") as file:
            df_master = pd.read_csv(file)



        df_master.columns = ["drcode","yjcode","drugname","3","housou","standard","6","stock","8", 
                         "9","10","11","12","13","14","15","16","17","18","19","20","21",
                         "22","23","24","25"]    
        df_master = df_master.drop(["3","6","8","9","10","11","12","13","14","15","16","17",
                                "18","19","20","21","22","23","24","25"],axis=1)

        #print "column name after dropping unnecessary columns:\n", df_master.columns
        #df_master["standard"] = df_master.loc[:,"standard"].str.decode('cp932')
        #df_master["drugname"] = df_master.loc[:,"drugname"].str.decode('cp932')
        #df_master["newcode"]= df_master["drugname"] + df_master["standard"]
        
        df_master["newcode"] = df_master.loc[:,"drcode"].astype(str) + df_master.loc[:,"housou"].astype(str)
        #df_master["newcode"] = df_master.loc[:,"newcode"].astype(np.long)


        #print(df_master.head())

        #df_master["newcode"] = df_master.loc[:,"newcode"].astype(np.int32)
        
        df_master["drugcode"]= df_master.loc[:,"drugname"] + df_master.loc[:,"standard"]

        #        
        #self.df_masterData = df_master.set_index("newcode")
        #
        self.df_masterData = df_master
        
        #if self.test:
        #    self.printHeader()
    
    def getStock(self):
        
        myColumnlist = ["drugcode","newcode","stock","yjcode","standard","drugname"]
        df_ = self.df_masterData.loc[:,myColumnlist]
        return df_

    def merge(self,df_user,df_rack):

        df_merge = pd.merge(df_user,self.getDfMasterData(),on='newcode',how='left')
        
        df_merge['num'] = df_merge['num'].fillna(0)

        df_merge['final'] = df_merge['stock'] - df_merge['num']
        
        df_merge['drugcode'] = df_merge.loc[:,'drugname'] + df_merge.loc[:,'standard']

        df_merge = pd.merge(df_merge,df_rack,on='drugcode',how='left')
        
        
        cols = ["rack","drugname","standard","final"]
        df_merge = df_merge.loc[:,cols]
        df_merge = df_merge.sort_values(by=["rack","drugname"])

        return df_merge
        
        
        
        
        
        
        

        
        
