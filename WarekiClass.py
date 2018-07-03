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

class WarekiClass(object):
    
    def __init__(self):
        
        self.date_ops = lambda x:pd.datetime.strptime(x,"%Y/%m/%d")
    
    def getDate(self,indata):
        
        syymmdd = str(indata)
        self.wareki = syymmdd
        
        self.gengo = syymmdd[0]
        self.yy = syymmdd[1:3]
        self.mm = syymmdd[3:5]
        self.dd = syymmdd[5:7]
        
        if self.gengo == "2":
            # taisho
            self.taisho(syymmdd)

        if self.gengo == "3":
            # taisho
            self.showa(syymmdd)

        if self.gengo == "4":
            # taisho
            self.heisei(syymmdd)
            
        self.yyyymmdd = self.date_ops(self.yyyymmdd_str)

        return self.yyyymmdd
    
    def convertYYYYMMDD(self,year):
        
        self.yyyymmdd_str = str(year) + "/"+ self.mm + "/" + self.dd
            
    def taisho(self,syymmdd):
        base = 1911
        year = base + int(self.yy)
        self.convertYYYYMMDD(year)

    def showa(self,syymmdd):
        base = 1925
        year = base + int(self.yy)
        self.convertYYYYMMDD(year)
        
    def heisei(self,syymmdd):
        base = 1988
        year = base + int(self.yy)
        self.convertYYYYMMDD(year)
    
