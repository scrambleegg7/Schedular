import numpy as np
import pandas as pd
import os
from datetime import date as dt

import seaborn as sns
from time import time
import matplotlib.pyplot as plt

import string
import hashlib

import csv
import codecs
import boto3
from decimal import *

from WarekiClass import WarekiClass
from ReceiptyClass import ReceiptyClass

from pymongo import MongoClient
from mongo_connect import ReceptyMongo

from bson.decimal128 import Decimal128


import platform


def connectMongDB():

    mongoObj = ReceptyMongo()
    mongoObj.setSchedularTable()

    return mongoObj


def clearMongoDB(data_dir):

    #table = connectDynamoDBTable()

    mongoObj = connectMongDB()

    ret = mongoObj.delete_many()
    print("[insertLGSchedularBatch]  counting schedular table -->   %s " % ret  ) 

    num_data = mongoObj.count()
    print("[insertLGSchedularBatch]  counting schedular table -->   %d " % num_data  ) 


def main():

    if platform.system() == "Windows":
        #recepty_data_dir = "L:/RECEPTY"
        data_dir = "L:/ipython"
    else:
        data_dir = "/Volumes/myShare/ipython"

    clearMongoDB(data_dir)

if __name__ == "__main__":

    main()
