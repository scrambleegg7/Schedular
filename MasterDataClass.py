# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

import sys

import glob

class MasterDataClass(object):
    
    def __init__(self,udir,test=False):
        
        self.udirPath = udir
        #print "-- user directory path for wildcard %s:" % udirPath
        self.targets = glob.glob(self.udirPath)
        if not self.targets:
            print("-- Files are no longer existed : ---",self.udirPath)
            sys.exit()
        else:
            print( "-- %s --" % self.udirPath )
    
        self.targets.reverse()
            
        self.first = self.get_firstItem(self.targets)
        print( "   -- target filename:%s" % self.first )

        self.test = test
        
        self.df_masterData = None
        
        self.loadData()
    
    def setDfMasterData(self,df_userdefined):
        self.df_masterData = df_userdefined
    
    def getDfMasterData(self):
        return self.df_masterData
        
    def get_firstItem(self,iterable,default=None):
        if iterable:
            for item in iterable:
                return item
            return default
    
    def loadData(self):
        pass
    
    def printHeader(self):
        if self.test:
            print( self.df_masterData.head() )

