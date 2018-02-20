#!/bin/bash

# Load functions
for function in `ls ./functions/*.sh`; do . ${function}; done

# usage
#
# Prints script usage instructions
usage () {
  echo ""
  echo "./dynamodb_json_backup.sh -t table_name [-p iops -o csv_file -b bucket -g uid]"
  echo "    -t: Name of DynamoDB table to back up in JSON format to S3"
  echo "    -g: (optional) UID to which to Grant full ownership to backed up files"
  echo "    -b: (optional) S3 Bucket to write backup data. Data will be backed up to s3://<this-bucket>/<table-name>"
  echo "    -p: (optional) Read IOPS to use. Overrides calculations in the script."
  echo "    -o: (optional) Print results to CSV file."
  echo "    -h: print this help message"
}

override_iops=0
MAX_READ_IOPS=40000
DYNAMODB_READ_UNIT_PRICE=0.00013
db=ddb_backup_verify

while getopts t:b:g:p:o:h opt; do
  case ${opt} in
    t) table_name=${OPTARG};;
    b) s3_bucket=${OPTARG};;
    g) grant_canonical_uid=${OPTARG};;
    p) override_iops=$OPTARG;;
    o) csv_file=${OPTARG};;
    h)
      usage
      exit 0
      ;;
    \?)
      exit 1
      ;;
    :)
      usage
      exit 1
      ;;
  esac
done

# If the table to backup hasn't been passed, exit.
if [ "$table_name" == "" ]; then
  echo "Required parameter missing: table name."
  usage
  exit 1
fi

if [[ -z ${s3_bucket+x} ]]; then
  s3_bucket=jornaya-data
  if [[ ${table_name} =~ _[0-9]{4}$ ]]; then
    mmyy_suffix=`echo $table_name | awk -F _ '{print $(NF)}'`
    message_type=${table_name:0:${#table_name}-5}
    month=${mmyy_suffix:0:2}
    year=20${mmyy_suffix:2:2}
    partition="month=$year-$month-01"
    s3_location=s3://${s3_bucket}/json/${message_type}/${partition}/ # Default S3 location (if not supplied)
  else
    s3_location=s3://${s3_bucket}/json/${table_name}/ # Default S3 location (if not supplied)
  fi
else
  s3_location=s3://${s3_bucket}/${table_name}/
fi

pipeline_name="DynamoDB JSON S3 Backup [${table_name}]" # Name for Data Pipeline

# Raise DynamoDB table Read Capacity Units (RCU's)
get_table_info $table_name
make_dynamodb_rcu_script
if [ $new_read_iops -gt $read_capacity ]; then
  echo `date` Raise IOPS on $table from $read_capacity to $new_read_iops \($`echo "$new_read_iops * $DYNAMODB_READ_UNIT_PRICE" | bc` per hour\)
else
  echo 'Read IOPS less than Read Capacity. IOPS will not be raised'
fi
. ./$change_dynamodb_rcu_script_name $new_read_iops
rm $change_dynamodb_rcu_script_name

# Create Data Pipeline
pipeline_id=$(aws datapipeline create-pipeline --name "${pipeline_name}" --unique-id `uuidgen` --query 'pipelineId' --output text)

if [[ $? -ne 0 ]]; then
  echo `date` "Data Pipeline creation failed! Exiting..."
  exit 1
fi

# Make data pipeline definition
data_pipeline_definition=${pipeline_id}.json
sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" data_pipeline_template.json | sed "s|__S3_LOCATION__|${s3_location}|g" > $data_pipeline_definition

# Publish pipeline definition
aws datapipeline put-pipeline-definition --pipeline-id $pipeline_id --pipeline-definition file://${data_pipeline_definition}

if [[ $? -ne 0 ]]; then
  echo `date` "Data Pipeline definition failed! Exiting..."
  exit 1
fi

# Remove local data pipeline definition
rm $data_pipeline_definition

# Activate pipeline
aws datapipeline activate-pipeline --pipeline-id $pipeline_id

if [[ $? -ne 0 ]]; then
  echo `date` "Data Pipeline activation failed! Exiting..."
  exit 1
fi

# Wait until work by EMR cluster moving DynamoDB data to S3 started by Data Pipeline is done
result=$(wait_for_pipeline)

# Restore provisioned DynamoDB IOPS
if [ $new_read_iops -gt $read_capacity ]; then
  echo $($date_command) Restore IOPS on $table_name from $new_read_iops to $read_capacity \($`echo "$read_capacity * $DYNAMODB_READ_UNIT_PRICE" | bc` per hour\)
  aws dynamodb update-table --table-name $table_name --provisioned-throughput ReadCapacityUnits=$read_capacity,WriteCapacityUnits=$write_capacity
else
  echo 'IOPS were not raised. Nothing to lower.'
fi

# Delete pipeline
aws datapipeline delete-pipeline --pipeline-id $pipeline_id

if [[ $? -ne 0 ]]; then
  echo `date` "Data Pipeline deletion failed! Exiting..."
  exit 1
fi

if [[ ${result} -ne 0 ]]; then
  echo `date` "Date Pipeline activity did not finish successfully. Exiting..."
  exit 1
fi

# Get the path of the latest written backup
new_key=$(get_latest_s3_key ${s3_location})

# Grant access to the newly written key if we're handling cross account buckets
if [[ ! -z ${grant_canonical_uid+x} ]]; then
  for object in $(aws s3api list-objects --bucket ${s3_bucket} --prefix ${table_name}/${new_key} \
                  --query Contents[].Key --output text); do
    aws s3api put-object-acl --bucket ${s3_bucket} --key ${object} --grant-full-control "id=${grant_canonical_uid}"
  done
fi

# Remove everything but the latest backup
aws s3 rm ${s3_location} --recursive --include '*' --exclude "*${new_key}*"

# Remove the "manifest" and "SUCCESS" file since we don't want this metadata commingling with
# our data (recommended by AWS in leadidaws account support case 4713814881)
aws s3 rm ${s3_location} --recursive --exclude '*' --include '*manifest*'
aws s3 rm ${s3_location} --recursive --exclude '*' --include '*SUCCESS*'

sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_create.ddl | sed "s|__S3_LOCATION__|${s3_location}|g" > ${table_name}-create.ddl
sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_drop.ddl > ${table_name}-drop.ddl
sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_count.ddl > ${table_name}-count.ddl

run_athena_query "$(< ${table_name}-drop.ddl)" ${db}
run_athena_query "$(< ${table_name}-create.ddl)" ${db}
run_athena_query "$(< ${table_name}-count.ddl)" ${db}
row_count=$(aws athena get-query-results --query-execution-id ${query_id} --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text)

rm ${table_name}-drop.ddl ${table_name}-create.ddl ${table_name}-count.ddl

exit_status=0
if [[ ${row_count} -ne ${item_count} ]]; then
  echo `date` "DynamoDB item count does not equal S3 row count."
  echo "DynamoDB Item Count: ${item_count}"
  echo "S3 Row Count: ${row_count}"
  exit_status=1
fi

cd verification_scripts
output=$(pipenv run python dynamodb_backup_test.py --table ${table_name})

if [[ $? -ne 0 ]]; then
  exit_status=1
  echo `date` "Verification failed! ${output}"
fi

cd ..

if [[ ! -z ${csv_file+x} ]]; then
  printf '%s\n' ${table_name} "$(date)" ${item_count} ${row_count} ${exit_status} | gpaste -sd ',' >> ${csv_file}
  aws s3 cp ${csv_file} s3://jornaya-data/${csv_file}
fi

echo ${date_command} "Exit status is ${exit_status}"
exit ${exit_status}
