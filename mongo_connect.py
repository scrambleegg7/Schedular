import numpy as np
import pandas as pd   

from datetime import datetime

from pymongo import MongoClient
from pymongo import errors


# mongodb+srv://<username>:<password>@mymongodbcluster.k44wh.mongodb.net/<dbname>?retryWrites=true&w=majority

class ReceptyMongo(object):

    def __init__(self):
        self.clint = MongoClient("mongodb+srv://dbuser:dbuser@mymongodbcluster.k44wh.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.clint.get_database("em_recepty")

    def setSchedularTable(self):
        self.table = self.db.schedular

    def add_one(self, post):
        """データ挿入"""
        #post = {
        #    'title': 'ハリネズミ',
        #    'content': 'ハリネズミ可愛い~',
        #    'created_at': datetime.now()
        #}
        return self.table.insert_one(post)

    def add_many(self, posts):
        """ insert bulk data """

        if not posts:
            print("")
            print("** [add_many blank !!!] **")
            return

        try:
            res = self.table.insert_many(posts)

        except errors.BulkWriteError as e:
            print( e.details['writeErrors'] )
            

        except Exception as e:
            print("")
            print("** [add_many error] **")
            print("posts---> %s" % posts)
            
            

        return res

    def count(self):

        return self.table.find().count()

    
    def delete_many(self, post = { } ):

        return self.table.delete_many( post )

    def findAndLimit(self,query = {}):

        query = self.table.find(query).limit(1)

        output = {} 
        i = 0
        for x in query: 
            output[i] = x 
            output[i].pop('_id') 
            i += 1

        return output

    def findOne(self, argument, queryObject = {}): 
        #queryObject = {argument: value} 
        query = self.table.find_one(queryObject) 
        query.pop('_id') 
        return query 

    def findAll(self): 
        query = self.table.find() 
        output = {} 
        i = 0
        for x in query: 
            output[i] = x 
            output[i].pop('_id') 
            i += 1
        return output 

    def update(self, key, value, element, updateValue): 
        queryObject = {key: value} 
        updateObject = {element: updateValue} 
        query = self.table.update_one(queryObject, {'$set': updateObject}) 
        if query.acknowledged: 
            return "Update Successful"
        else: 
            return "Update Unsuccessful"


def main():
    obj = ReceptyMongo()
    #rest = obj.add_one()
    #print(rest)

    rest = obj.findOne("title","ハリネズミ")
    print(rest)
    

    rest = obj.update("title","ハリネズミ","content","Siberian Husky." )
    print(rest)


    rest = obj.findAll()
    print(rest)



if __name__ == '__main__':
    main()