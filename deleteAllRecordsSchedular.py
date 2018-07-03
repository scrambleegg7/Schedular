
# coding: utf-8
import sys
import os
import boto3

REGION_NAME = "ap-northeast-1"
TABLE_NAME  = "sample-table"

def connectDynamoDBTable():

    dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
    table = dynamodb.Table('schedular')
    print("TABLE creation date:",table.creation_date_time) 

    return table

def truncate_dynamo_items(dynamodb_table):

    # get all records from table 
    delete_items = []
    parameters   = {}
    while True:
        response = dynamodb_table.scan(**parameters)
        delete_items.extend(response["Items"])
        if ( "LastEvaluatedKey" in response ):
            parameters["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    # pickup keyScheme
    key_names = [ x["AttributeName"] for x in dynamodb_table.key_schema ]
    delete_keys = [ { k:v for k,v in x.items() if k in key_names } for x in delete_items ]

    # データ削除
    with dynamodb_table.batch_writer() as batch:
        for key in delete_keys:
            batch.delete_item(Key = key)

    return 0



def main():
    table    = connectDynamoDBTable()
    truncate_dynamo_items(table)

    return 0



if __name__ == '__main__':
    ret = main()
    sys.exit(ret)
