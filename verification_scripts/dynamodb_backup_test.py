import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import decimal
import pprint
import argparse
import time


parser = argparse.ArgumentParser(description='An automated script to verify successful backup of dynamodb tables')

###Command Line Arguments###
parser.add_argument('--table', required=True)
parser.add_argument('--count', required=True)
args = parser.parse_args()
table_name = args.table.lower()
row_count = args.count

####AWS Objects####
athena = boto3.client('athena')
dynamo = boto3.resource('dynamodb')
dynamodb_table = dynamo.Table(table_name)

###Query Athena###
response = athena.start_query_execution(
    QueryString='SELECT * FROM ddb_backup_verify.{} limit 10'.format(table_name),
    QueryExecutionContext={
      'Database': 'ddb_backup_verify'
    },
    ResultConfiguration={
      'OutputLocation': 's3://aws-athena-query-results-298785453590-us-east-1'
    }
)

execution_id = response['QueryExecutionId']

###Check Execution Status of Athena Query###
queryStatus = athena.get_query_execution(
    QueryExecutionId=execution_id
)

print(queryStatus['QueryExecution']['Status']['State'])

while queryStatus['QueryExecution']['Status']['State'] == 'RUNNING':
  queryStatus = athena.get_query_execution(
    QueryExecutionId=execution_id
  )
  print(queryStatus['QueryExecution']['Status']['State'])
  time.sleep(10)
else:
  print (queryStatus['QueryExecution']['Status']['State'], " Query Status")


####Convert to Dictionary####
def dynamodb_dictionary_to_python_dictionary(dynamo_dictionary):
    for key, value in dynamo_dictionary.items():
        dynamo_type = list(value.keys())[0]

        if type(value[dynamo_type]) == dict:
            dynamo_dictionary[key] = dynamodb_dictionary_to_python_dictionary(value[dynamo_type])
        elif dynamo_type == 'n':
            dynamo_dictionary[key] = decimal.Decimal(value[dynamo_type])
        else:
            dynamo_dictionary[key] = value[dynamo_type]
    return dynamo_dictionary

###Athena Results####
queryResult = athena.get_query_results(QueryExecutionId=execution_id)
athena_results = []
token_list = []
for row in queryResult["ResultSet"]["Rows"][1:]:
    current_row = json.loads(row["Data"][0]["VarCharValue"])
    current_row_python = dynamodb_dictionary_to_python_dictionary(current_row)
    token_list.append(current_row_python['token'])
    athena_results.append(current_row_python)

####Query DynamoDB###
dynamo_results = []
for item in token_list:
    response = dynamodb_table.query(KeyConditionExpression=Key('token').eq(item))
    dynamo_results.append(response['Items'][0])

####Verify Results####
print(athena_results == dynamo_results)
