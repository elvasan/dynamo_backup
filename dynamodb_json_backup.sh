#!/bin/bash

# Load functions
for function in `ls ./functions/*.sh`; do . ${function}; done

# usage
#
# Prints script usage instructions
usage () {
  echo ""
  echo "./dynamodb_json_backup.sh -t table_name"
  echo "    -t: Name of DynamoDB table to back up in JSON format to S3"
  echo "    -b: (optional) S3 Path to logs bucket"
  echo "    -p: (optional) Read IOPS to use. Overrides calculations in the script."
  echo "    -h: print this help message"
}

override_iops=0
MAX_READ_IOPS=40000
DYNAMODB_READ_UNIT_PRICE=0.00013
db=ddb_backup_verify

while getopts t:b:p:h opt; do
  case ${opt} in
    t)
    table_name=${OPTARG}
    # Ensure backups are written to appropriate S3 partition for monthly sharded tables
    if [[ $table_name =~ _[0-9]{4}$ ]]; then
      mmyy_suffix=`echo $table_name | awk -F _ '{print $(NF)}'`
      message_type=${table_name:0:${#table_name}-5}
      month=${mmyy_suffix:0:2}
      year=20${mmyy_suffix:2:2}
      partition="month=$year-$month-01"
      s3_location=s3://jornaya-data/json/${message_type}/${partition}/ # Default S3 location (if not supplied)
    else
      s3_location=s3://jornaya-data/json/${table_name}/ # Default S3 location (if not supplied)
    fi
    ;;
    b) s3_location=${OPTARG};;
    p) override_iops=$OPTARG;;
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

pipeline_name="DynamoDB JSON S3 Backup [${table_name}]" # Name for Data Pipeline

# Raise DynamoDB table Read Capacity Units (RCU's)
get_table_info $table_name
make_dynamodb_rcu_script
if [ $new_read_iops -gt $read_capacity ]; then
  echo `date` Raise IOPS on $table from $read_capacity to $new_read_iops \($`echo "$new_read_iops * $DYNAMODB_READ_UNIT_PRICE" | bc` per hour\)
else
  echo 'Proposed new Read IOPS less than or equal to current Read Capacity. IOPS will not be raised'
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

# Wait until work by EMR cluster moving DynamoDB data to S3 orchestrated by Data Pipeline is done
wait_for_pipeline

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

# Remove the "manifest" file since we don't want this metadata commingling with our data (recommended by AWS in leadidaws account support case 4713814881)
aws s3 rm $s3_location --recursive --exclude "*" --include "*manifest"

sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_create.ddl | sed "s|__S3_LOCATION__|${s3_location}|g" > ${table_name}-create.ddl
sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_drop.ddl > ${table_name}-drop.ddl
sed "s/__DYNAMODB_TABLE_NAME__/${table_name}/g" athena_verification_count.ddl > ${table_name}-count.ddl

run_athena_query "$(< ${table_name}-drop.ddl)" ${db}
run_athena_query "$(< ${table_name}-create.ddl)" ${db}
run_athena_query "$(< ${table_name}-count.ddl)" ${db}
s3_row_count=$(aws athena get-query-results --query-execution-id ${query_id} --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text)
run_athena_query "$(< ${table_name}-drop.ddl)" ${db}

rm ${table_name}-drop.ddl ${table_name}-create.ddl ${table_name}-count.ddl

if [[ ${s3_row_count} -ne ${item_count} ]]; then
  echo `date` "DynamoDB item count does not equal S3 row count."
  echo "DynamoDB Item Count: ${item_count}"
  echo "S3 Item Count: ${s3_row_count}"
  exit 1
else
  exit 0
fi
