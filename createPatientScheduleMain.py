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

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass


def loadCurrentData():

    date_ops = lambda x:pd.datetime.strptime(x,"%Y-%m-%d")

    filename = os.path.join("/Volumes/myShare/ipython","longterm.csv")

    df_lg = pd.read_csv(filename,encoding="Shift_JISx0213")
    col_names = [ 'c{0:02d}'.format(i) for i in range( len( df_lg.columns ) ) ]

    df_lg.columns = col_names

    df_lg.c04 = df_lg.c04.apply(date_ops)
    df_lg.c07 = df_lg.c07.apply(date_ops)

    mask = df_lg.c09.notnull()

    uniq_date = sorted( df_lg.c09[mask].unique() )

    prv2_date = uniq_date[-2]
    min_prv_date = min( df_lg.c09[mask]  )
    max_prv_date = max( df_lg.c09[mask]  )
    df_lg.c09 = df_lg.c09.fillna( min_prv_date )

    cols = ["name","mark","hospital","medicine","nextDate","total_amount","mark2","exp","standard","czDate"]
    df_lg.columns = cols

    print("* max date (previous date of latest date)  ..", prv2_date)
    return df_lg, prv2_date

def main():

    receptyCls = ReceiptyClass()

    df_merge = receptyCls.receiptyFlatten()
    print("* shape of tokyo/national integrated date", df_merge.shape)
    
    df_lg, max_prv_date = loadCurrentData()
    df_merge = df_merge[df_merge.czDate > max_prv_date]
    mask = df_merge.hospital.str.contains('北桜') 
    df_merge = df_merge[~mask].sort_values(["nextDate","name"])

    print("* shape of tokyo/national after removing overlapping date..", df_merge.shape)

    if (df_merge.shape[0] == 0):
        print(" no selected data bigger than ", max_prv_date)
        return

    data_dir = "/Volumes/myShare/ipython"
    integrate_csvfile = os.path.join(data_dir, "integrate2.csv")
    df_merge.to_csv(integrate_csvfile, index=False, encoding="cp932")

    
    df_lg =df_lg.append(df_merge)
    df_lg.czDate = pd.to_datetime( df_lg.czDate )
    longterm_csvfile = os.path.join(data_dir, "longterm.csv")
    df_lg.to_csv(longterm_csvfile, index=False, encoding="cp932")
    


if __name__ == "__main__":

    main()
