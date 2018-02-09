import argparse
import base64
import decimal
import json
from time import sleep
import boto3
import sys

# Command Line Arguments
parser = argparse.ArgumentParser(description='An automated script to verify successful backup of dynamodb tables')

parser.add_argument('--table', required=True)
args = parser.parse_args()
table_name = args.table.lower()

# Convert dictionary made from dumping dynamodb json to a normal dictionary
def dynamodb_dictionary_to_python_dictionary(dynamo_dictionary):
    return_dictionary = {}

    for key, value in dynamo_dictionary.items():
        dynamo_type = list(value.keys())[0]
        if dynamo_type == 'l':
            if list(value[dynamo_type][0].keys())[0] == 'm':
                return_dictionary[key] = []
                for list_item in value[dynamo_type]:
                    return_dictionary[key].append(dynamodb_dictionary_to_python_dictionary(list_item['m']))
            else:
                return_dictionary[key] = value[dynamo_type]
        elif dynamo_type == 'm':
            return_dictionary[key] = dynamodb_dictionary_to_python_dictionary(value[dynamo_type])
        elif dynamo_type == 'n':
            return_dictionary[key] = decimal.Decimal(value[dynamo_type])
        elif dynamo_type == 'b':
            return_dictionary[key] = base64.b64decode(value[dynamo_type])
        elif dynamo_type == 'nS':
            return_dictionary[key] = set(map(decimal.Decimal, value[dynamo_type]))
        else:
            return_dictionary[key] = value[dynamo_type]

    return return_dictionary


# AWS Objects
athena = boto3.client('athena')
dynamo = boto3.resource('dynamodb')
dynamodb_table = dynamo.Table(table_name)
table_attributes = dynamodb_table.attribute_definitions

# Determine if we're validating all data for QA or checking a subset in Prod
if table_name[:6] == '_test_':
    record_limit = ''
else:
    record_limit = ' ORDER BY RANDOM() LIMIT 50'  # TODO: Is 50 enough, too much, or too little?

# Query Athena
response = athena.start_query_execution(
   QueryString='SELECT * FROM ddb_backup_verify.{}{}'.format(table_name, record_limit),
   QueryExecutionContext={
     'Database': 'ddb_backup_verify'
   },
   ResultConfiguration={
     'OutputLocation': 's3://aws-athena-query-results-298785453590-us-east-1'
   }
)

execution_id = response['QueryExecutionId']

# Check Execution Status of Athena Query
query_status = athena.get_query_execution(
   QueryExecutionId=execution_id
)

print('Athena Query Execution Started')

while query_status['QueryExecution']['Status']['State'] == 'RUNNING':
    query_status = athena.get_query_execution(
       QueryExecutionId=execution_id
    )
    print('Athena Query Status: '+query_status['QueryExecution']['Status']['State'])
    sleep(10)
else:
    print ('Athena Query Status: '+query_status['QueryExecution']['Status']['State'])

# Athena Results
query_result = athena.get_query_results(QueryExecutionId=execution_id)
athena_results = []

for row in query_result["ResultSet"]["Rows"][1:]:
    current_row = json.loads(row["Data"][0]["VarCharValue"])
    current_row_python = dynamodb_dictionary_to_python_dictionary(current_row)
    athena_results.append(current_row_python)

while 'NextToken' in query_result:
    query_result = athena.get_query_results(QueryExecutionId=execution_id,
                                            NextToken=query_result['NextToken'])
    for row in query_result["ResultSet"]["Rows"]:
        current_row = json.loads(row["Data"][0]["VarCharValue"])
        current_row_python = dynamodb_dictionary_to_python_dictionary(current_row)
        athena_results.append(current_row_python)

# Query DynamoDB
failed_records = 0
for record in athena_results:
    table_keys = {}

    for attribute in dynamodb_table.attribute_definitions:
        table_keys[attribute['AttributeName']] = record[attribute['AttributeName']]

    dynamo_record = dynamodb_table.get_item(Key=table_keys)

    if dynamo_record['Item'] != record:
        print('The Following Records Failed Validation')
        print('Dynamo Record')
        print(dynamo_record['Item'])
        print('Athena Record')
        print(record)
        failed_records += 1

if failed_records != 0:
    sys.exit(1)
