#coding: utf-8

from datetime import datetime
from datetime import date
from datetime import timedelta
import sys

class MyDateClass(object):

    def __init__(self):
        self.TDY = date.today() 
        #self.TDY = datetime.today() 
        self.YYYYMMDD = None
        self.MM = None
        self.YYYYMM = None
        self.ymd = None
    
    def getToday(self):
        return self.TDY
    
    def setDate(self,ustr):
        self.ymd = ustr.split('/')
        self.TDY = date(int(self.ymd[0]),int(self.ymd[1]),int(self.ymd[2]))
        return self.TDY
        
    def strYYYYMMDD(self):
        self.YYYYMMDD = self.TDY.strftime('%Y%m%d')
        return self.YYYYMMDD
    
    def strMM(self):
        self.MM = self.TDY.strftime('%m')
        return self.MM
    
    def strYYYYMM(self):
        self.YYYYMM = self.TDY.strftime('%Y-%m')
        return self.YYYYMM
        
    def strYYYY(self):
        self.YYYY = self.TDY.strftime('%Y')
        return self.YYYY

        
    def timeDeltaDays(self,idays):
        try:
            dt = timedelta(days=idays)
            t = self.TDY - dt
            return t
        except:
            print(" *** Unexpected error (MyDataClass - timeDeltaDays) *** :", sys.exc_info()[0])

    
def main():
    mObj = MyDateClass()
    #print mObj.setStrDate('2014/01/05')
    
if __name__ == '__main__':
    main()