# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

import sys
import codecs
import glob

from MasterDataClass import MasterDataClass


class InputDataClass(MasterDataClass):
    
    def __init__(self,udir,test=False,tobuSw=False):
        
        super(InputDataClass,self).__init__(udir,test)
        
        self.tobuclinic =  u"東武練馬ｸﾘﾆｯｸ"
        #self.packmachine = u"分包機-1-"
        self.packmachine = u"分包機"

        self.df_reindexed = None
        
        self.tobuSw = tobuSw
        
    def loadData(self):
        
        with codecs.open(self.first, "r", "Shift-JIS", "ignore") as file:
            df_in = pd.read_csv(file, dtype="object")

        # due to TAX increase from 8% to 10%
        # 1 new field indicating new TAX category was added by EM systems
        #
        #   

        df_in.columns = ["0","indate","drcode","housou","standard","box","num","7","8","wholesaler","jancode","price","drugname","13","14","yjcode","16","17","18","19","20"]    
        target_col = ["indate","drcode","housou","standard","box","num","wholesaler","jancode","price","drugname","yjcode"]
        df_in = df_in[target_col]

        df_in["indate"] = pd.to_datetime(df_in["indate"])    
        #df_out["newcode"]= df_out["drugname"] + df_out["standard"]

        df_in["newcode"] = df_in.loc[:,"drcode"].astype(str) + df_in.loc[:,"housou"].astype(str) 
        #df_out["newcode"] = df_out["newcode"].astype(np.long)

        df_in["drugcode"]= df_in.loc[:,"drugname"] + df_in.loc[:,"standard"]

        #df_in.jancode = df_in.jancode.astype(int)

        target_col = ["indate","standard","box","num","wholesaler","jancode","drugname","yjcode"]
        df_in = df_in[target_col]
        #df_in.jancode = df_in.jancode.fillna(0).astype(int)
        #df_in.jancode = df_in.jancode.apply(  pd.to_numeric  )
        df_in.jancode = pd.Series( df_in.jancode , dtype =      pd.Int64Dtype() )

        df_in.box = df_in.box.apply(  pd.to_numeric  )
        df_in.num = df_in.num.apply(  pd.to_numeric  )



        self.df_masterData = df_in
        
        if self.test:
            self.printHeader()
        
    def groupByDrugName(self,df_othermaster,YYYYMMDD):
        
        
        cols = ["newcode","outdate","drugname","standard","instname","num","drugcode"]
        df_ = self.df_masterData.loc[:,cols]
        

        if self.tobuSw == "only":
            print( df_.head()  )
            #print(  df_["instname"] )
            print(  self.tobuclinic  )
            df_select = df_[df_["instname"] == self.tobuclinic]
        
        elif self.tobuSw == "excl":
            df_select = df_[df_["instname"] != self.tobuclinic]
            #df_ = df_[df_["outdate"] > pd.to_datetime(YYYYMMDD)]
        else:
            df_select = df_
            #df_ = df_[df_["outdate"] > pd.to_datetime(YYYYMMDD)]
        
        # calculate total output number from forward date YYYYMMDD (eg. 20161201 )
        df_forward_data = df_[  df_["outdate"] > pd.to_datetime(YYYYMMDD)  ] 
        sum_ = df_forward_data.groupby('newcode')['num'].sum()        
        df_summary = pd.DataFrame(sum_)
        # move index key into column field and reindex        
        
        df_summary.reset_index(inplace=True)
        #print(df_summary.head())
        #df_summary['newcode'] = df_summary.index

 
        # only group by drugcode = drugname + housou from all output data        
        drugcode_group = [   l[0] for l in df_select.groupby('drugcode')      ]
        df_drugcode_only = pd.DataFrame( drugcode_group,columns=["drugcode"]  )
        
        
        cur_df = pd.merge(df_drugcode_only,df_othermaster,on='drugcode',how='left')    
        #cur_df.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\mytest.csv',encoding='cp932',index=False)
        
        # merge with forward data if exists
        df_sum = pd.merge(cur_df,df_summary,on='newcode',how='left')    
        df_sum['num'] = df_sum['num'].fillna(0)
        df_sum['final'] = df_sum['stock'] - df_sum['num']

        
        df_sum.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\checkStockData.csv',encoding='cp932',index=False)
        
        self.df_sum = df_sum
        
        return True if len(df_sum) > 0 else False
        
    
    def mergeRackData(self,df_rack):


        df_merge = pd.merge(self.df_sum,df_rack,on='drugcode',how='left')
        

        if self.tobuSw == "bu":

            print("tab calculation...")
            cols = ["rack","drugname","standard","final", "num"]
            df_merge = df_merge.loc[:,cols]
            df_merge = df_merge.sort_values(by=["rack","drugname"])

            #df_merge = df_merge[  df_merge["rack"] == self.packmachine ] 
            df_merge = df_merge[  df_merge["rack"].str.contains(self.packmachine,na=False) ] 

        else:

            cols = ["rack","drugname","standard","final"]
            df_merge = df_merge.loc[:,cols]
            df_merge = df_merge.sort_values(by=["rack","drugname"])


        return df_merge
        
         
        
    def dataSummerize(self):
        
        cols = ["outdate","drugname","standard","instname","num"]
        self.df_masterData = self.df_masterData.loc[:,cols]

        self.df_masterData.reset_index(level=0,inplace=True)
        
        
        # grouped by newcode == drugname + standard        
        grouped = self.df_masterData.groupby("newcode")
        
        #print "-- grouped:", grouped.head()
        index = [gp_keys[0] for gp_keys in grouped.groups.values()]
        print( "--- index ---", index )        
        
        #print "--- reindexed : ", self.df_masterData["newcode"].reindex(index).head()
        #print "--- column names", pd.DataFrame( self.df_masterData["newcode"].reindex(index) ).columns
        
        # reindexed with grouped newcode                
        tempdf = pd.DataFrame( self.df_masterData["newcode"].reindex(index) )
        tempdf = tempdf.sort_values(by="newcode")
#        tempdf = tempdf.set_index("newcode")
        print(  "-- temp df", tempdf.head()   )      
        
        self.df_reindexed = tempdf
        
        
        #self.df_masterData = unique_df.set_index("newcode")



        #self.printHeader()

    def mergeWithOther(self,df_othermaster):
        
        
        cur_df = self.df_reindexed.join(df_othermaster["stock"],how="inner")
        print( "-- merged --" )
        print( cur_df.head() )
        pass