import boto3

dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
table = dynamodb.Table('schedular')

print(table.creation_date_time) 

table.delete()
