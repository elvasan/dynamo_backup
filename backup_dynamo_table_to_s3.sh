#!/bin/bash

# backup_dynamodb_table_to_s3.sh
#
# Uses EMR and Hive queries to transfer a DynamoDB table to an S3 bucket.

# Set some constants.
BACKUP_HQL="ddb_backup.hql"
S3_WRITE_HQL="s3_write_no_partition.hql"
S3_BUCKET=jornaya-backup
READ_PERCENTAGE="1.5"
DYNAMODB_READ_UNIT_PRICE=0.00013
MAX_READ_IOPS=40000
SHARDED_TABLES=( "deviceid" "formdata" "lead_dom" "leads" "snapshots" "urls")

# Set initial and default values for variables.
date_suffix=""
table=""
table_no_mmyy=""
time_to_complete=3600
next_month=""
this_month=""
cluster_id=""
step_ids=""
override_iops=0
availability_zone="us-east-1e"
core_instance_type="d2.8xlarge"
spot_bid_price=2
debug_mode=false

# Set date command to use based on OS. Use gdate on Macs, date elsewhere.
[[ $OSTYPE =~ 'darwin' ]] && date_command='gdate' || date_command='date'

generateHiveBackupScript() {
  cat ${BACKUP_HQL} \
  | sed "s/TABLENAME/${table}/g" \
  | sed "s/NO_MMYY/${table_no_mmyy}/g" \
  | sed "s/READ_PERCENTAGE/${READ_PERCENTAGE}/g" \
  | sed "s/NEXT_MONTH/${next_month}/g" \
  | sed "s/THIS_MONTH/${this_month}/g" \
  > ${table}-backup.hql
}

generateS3WriteScript() {
  cat ${S3_WRITE_HQL} \
  | sed "s/TABLENAME/${table}/g" \
  | sed "s/NO_MMYY/${table_no_mmyy}/g" \
  | sed "s/S3_BUCKET/${S3_BUCKET}/g" \
  | sed "s/READ_PERCENTAGE/${READ_PERCENTAGE}/g" \
  | sed "s/START_DATE/${this_month}/g" \
  | sed "s/END_DATE/${next_month}/g" \
  > ${table}-s3-write.hql
}

ceiling() {
  echo "define ceil (x) {if (x<0) {return x/1} \
        else {if (scale(x)==0) {return x} \
        else {return x/1 + 1 }}} ; ceil($1)" | bc
}

generateThroughputScript() {
  cat > change_throughput_$table.sh <<EOF
#!/bin/bash

current_read=\$(aws dynamodb describe-table --table-name $table | jq -r .Table.ProvisionedThroughput.ReadCapacityUnits)
aws dynamodb update-table --table-name $table --provisioned-throughput ReadCapacityUnits=\$1,WriteCapacityUnits=$write_capacity

if [ \$current_read -gt \$1 ]; then
  echo "Not waiting for table to be active, exiting..."
  exit 0
fi

seconds_to_wait=10
table_state=UPDATING
while [ \$table_state != ACTIVE ]; do
  table_state=\$(aws dynamodb describe-table --table-name $table | jq -r '.Table.TableStatus')
  echo \$(date) Table $table state is \$table_state
  [ \$table_state != ACTIVE ] && echo Wait \$seconds_to_wait seconds...; sleep \$seconds_to_wait
done
EOF
}

getActualPartitions() {
  getCapacityPartitions
  getThroughputPartitions

  if [ $capacityPartitions -gt $throughputPartitions ]; then
    actualPartitions=$capacityPartitions
  else
    actualPartitions=$throughputPartitions
  fi
}

getCapacityPartitions() {
  tableSizeInGB=$(echo $table_size/1024/1024/1024 | bc)
  capacityPartitions=$(ceiling $tableSizeInGB/10)
}

getCompleteTimeThroughput() {

  completeTimeThroughput=$(echo $table_size/3600/8192 | bc)

  if [ $completeTimeThroughput -eq 0 ]; then
    completeTimeThroughput=1
  fi
}

getMaximumReadThroughput() {
  let "maximumReadThroughput = 3000 * $actualPartitions"
}

getNewReadIOPS() {
  if [ $override_iops -ne 0 ]; then
    new_read_iops=$override_iops
  elif [ $completeTimeThroughput -lt $maximumReadThroughput ]; then
    new_read_iops=$completeTimeThroughput
  else
    new_read_iops=$maximumReadThroughput
  fi

  if [ $new_read_iops -gt $MAX_READ_IOPS ]; then
    echo "Calculated read iops is greater than allowable maximum."
    echo "Setting read iops to $MAX_READ_IOPS."
    new_read_iops=$MAX_READ_IOPS
  fi
}

getThroughputPartitions() {
  throughputPartitions=$(echo $read_capacity / 3000 + $write_capacity / 1000 | bc)
  throughputPartitions=$(round $throughputPartitions 0)

  if [ $throughputPartitions -lt 1 ]; then
    throughputPartitions=1
  fi
}

getTableInfo() {
  describe_table=`aws dynamodb describe-table --table-name $1`
  read_capacity=`echo $describe_table | jq '.Table.ProvisionedThroughput.ReadCapacityUnits'`
  write_capacity=`echo $describe_table | jq '.Table.ProvisionedThroughput.WriteCapacityUnits'`
  table_size=`echo $describe_table | jq '.Table.TableSizeBytes'`
  item_count=`echo $describe_table | jq '.Table.ItemCount'`

  getActualPartitions
  getCompleteTimeThroughput
  getMaximumReadThroughput
  getNewReadIOPS

  printTableInfo
}

printTableInfo() {
  echo -e "Table:\t\t\t $table"
  echo -e "Current Read Capacity:\t $read_capacity"
  echo -e "Current Write Capacity:\t $write_capacity"
  echo -e "Item Count:\t\t $item_count"
  echo -e "Size (bytes):\t\t $table_size"
  echo -e "Estimated Partitions:\t $actualPartitions"
  echo -e "Maximum Read IOPS:\t $maximumReadThroughput"
  echo -e "Complete Time IOPS:\t $completeTimeThroughput"
  echo ""
}

round() {
  echo $(printf %.$2f $(echo "scale=$2;(((10^$2)*$1)+0.5)/(10^$2)" | bc))
};

uploadScriptsToS3() {
  scripts=( "${table}-backup.hql" "${table}-s3-write.hql" "change_throughput_$table.sh" )
  for i in "${scripts[@]}"
  do
    aws s3 cp ${i} s3://${S3_BUCKET}/${i}
  done
}

cleanupS3() {
  scripts=( "${table}-backup.hql" "${table}-s3-write.hql" "change_throughput_$table.sh" )
  for i in "${scripts[@]}"
  do
    aws s3 rm s3://${S3_BUCKET}/${i}
  done
}

provisionCluster() {
  if [ $new_read_iops -gt $read_capacity ]; then
    cluster_id=$(aws emr create-cluster --auto-terminate --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://${S3_BUCKET}/change_throughput_$table.sh","$new_read_iops"] Type=HIVE,Name='ddb-backup',ActionOnFailure=CONTINUE,ActionOnFailure=TERMINATE_CLUSTER,Args=[-f,s3://${S3_BUCKET}/$table-backup.hql] Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://${S3_BUCKET}/change_throughput_$table.sh","$read_capacity"] Type=HIVE,Name='s3-write',ActionOnFailure=CONTINUE,ActionOnFailure=TERMINATE_CLUSTER,Args=[-f,s3://${S3_BUCKET}/${table}-s3-write.hql] --applications Name=Hadoop Name=Hive Name=Pig Name=Hue --ec2-attributes '{"KeyName":"data","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"'${availability_zone}'","EmrManagedSlaveSecurityGroup":"sg-5632583e","EmrManagedMasterSecurityGroup":"sg-5432583c"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.7.1 --log-uri 's3n://aws-logs-298785453590-us-east-1/elasticmapreduce/' --name "$table backup" --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master instance group - 1"},{"InstanceCount":4,"BidPrice":"'${spot_bid_price}'","InstanceGroupType":"CORE","InstanceType":"'${core_instance_type}'","Name":"Core instance group - 5"}]' --region us-east-1 | jq -r .ClusterId)
  else
    cluster_id=$(aws emr create-cluster --auto-terminate --steps Type=HIVE,Name='ddb-backup',ActionOnFailure=CONTINUE,ActionOnFailure=TERMINATE_CLUSTER,Args=[-f,s3://${S3_BUCKET}/$table-backup.hql] Type=HIVE,Name='s3-write',ActionOnFailure=CONTINUE,ActionOnFailure=TERMINATE_CLUSTER,Args=[-f,s3://${S3_BUCKET}/${table}-s3-write.hql] --applications Name=Hadoop Name=Hive Name=Pig Name=Hue --ec2-attributes '{"KeyName":"data","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"'${availability_zone}'","EmrManagedSlaveSecurityGroup":"sg-5632583e","EmrManagedMasterSecurityGroup":"sg-5432583c"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.7.1 --log-uri 's3n://aws-logs-298785453590-us-east-1/elasticmapreduce/' --name "$table backup" --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master instance group - 1"},{"InstanceCount":4,"BidPrice":"'${spot_bid_price}'","InstanceGroupType":"CORE","InstanceType":"'${core_instance_type}'","Name":"Core instance group - 5"}]' --region us-east-1 | jq -r .ClusterId)
  fi
}

addStepsToCluster() {
  if [ $new_read_iops -gt $read_capacity ]; then
    total_steps=4
    step_ids=($(aws emr add-steps --cluster-id ${cluster_id} --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://${S3_BUCKET}/change_throughput_$table.sh","$new_read_iops"] Type=HIVE,Name='ddb-backup',ActionOnFailure=CANCEL_AND_WAIT,Args=[-f,s3://${S3_BUCKET}/$table-backup.hql] Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://${S3_BUCKET}/change_throughput_$table.sh","$read_capacity"] Type=HIVE,Name='s3-write',ActionOnFailure=CANCEL_AND_WAIT,Args=[-f,s3://${S3_BUCKET}/${table}-s3-write.hql] | jq -r .StepIds[]))
  else
    total_steps=2
    step_ids=($(aws emr add-steps --cluster-id ${cluster_id} --steps Type=HIVE,Name='ddb-backup',ActionOnFailure=CANCEL_AND_WAIT,Args=[-f,s3://${S3_BUCKET}/$table-backup.hql] Type=HIVE,Name='s3-write',ActionOnFailure=CANCEL_AND_WAIT,Args=[-f,s3://${S3_BUCKET}/${table}-s3-write.hql] | jq -r .StepIds[]))
  fi
}

usage () {
  echo ""
  echo "./backup_dynamodb_table_to_s3 -t table_name [-d MMYY -c seconds]"
  echo "    -t: DynamoDB table name to backup"
  echo "    -e: (optional) EMR Cluster ID to use for backup"
  echo "    -d: (optional) Date suffix to use for monthly sharded tables. If not set, current month and year will be used."
  echo "    -c: (optional) Time to complete, in seconds (default: 3600)"
  echo "    -b: (optional) Spot bid price, in dollars (default: \$2.00)"
  echo "    -i: (optional) EMR core instance type (default: d2.8xlarge)"
  echo "    -a: (optional) Availability Zone to be used for EMR cluster (default: us-east-1e)"
  echo "    -x: (optional) Enable debug mode. EMR cluster will not provisioned but throughput info will be printed."
  echo "    -p: (optional) Read IOPS to use. Overrides calculations in the script."
}

while getopts "xt:e:d:c:b:i:a:xp:" opt; do
  case $opt in
    a)
      availability_zone=$OPTARG
      ;;
    b)
      spot_bid_price=$OPTARG
      ;;
    c)
      time_to_complete=$OPTARG
      ;;
    d)
      date_suffix=$OPTARG
      ;;
    e)
      cluster_id=$OPTARG
      ;;
    i)
      core_instance_type=$OPTARG
      ;;
    p)
      override_iops=$OPTARG
      ;;
    t)
      table_no_mmyy=$OPTARG
      table=$table_no_mmyy
      ;;
    x)
      debug_mode=true
      echo "Debug mode enabled."
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      usage
      exit 1
      ;;
  esac
done

# If the table to backup hasn't been passed, exit.
if [ "$table" == "" ]; then
  echo "Required parameter missing: table name."
  usage
  exit 1
fi

if [[ " ${SHARDED_TABLES[@]} " =~ " ${table} " ]]; then
  echo "Table is sharded. Adding date suffix and adding partitioned S3 write..."
  S3_WRITE_HQL="s3_write.hql"
  date_suffix=`[ "$date_suffix" == "" ] && $date_command +%m%y || echo $date_suffix`
  [ ${#date_suffix} -gt 0 ] && table=$table"_"$date_suffix
fi

# Set variables to be applied as selection criteria for date based range queries
curr_year_month="20${date_suffix:2:2}-${date_suffix:0:2}"
this_month=$($date_command --date="$curr_year_month-01" +%Y-%m-%d)
next_month=$($date_command --date="$curr_year_month-15 +1 month" +%Y-%m-01)

# Special handling for the snapshots table.
if [[ $table =~ ^snapshots ]]; then
  echo "Using special DynamoDB backup and S3 write scripts for ${table}."
  BACKUP_HQL="snapshots_ddb_backup.hql"
  S3_WRITE_HQL="snapshots_s3_write.hql"
fi

getTableInfo $table
generateThroughputScript
generateHiveBackupScript
generateS3WriteScript
uploadScriptsToS3

[ $new_read_iops -gt $read_capacity ] && echo `date` Raise IOPS on $table from $read_capacity to $new_read_iops \($`echo "$new_read_iops * $DYNAMODB_READ_UNIT_PRICE" | bc` per hour\)

# Provision EMR Cluster
if [ $debug_mode != true ]; then
  echo ""
  if [ "$cluster_id" == "" ]; then
    echo "Starting EMR Cluster..."
    provisionCluster
    echo "Cluster ID: $cluster_id"

    # Wait until EMR Cluster is Ready
    seconds_to_wait=30
    cluster_state="STARTING"
    while [ $cluster_state != TERMINATING ]; do
      cluster_state=`aws emr describe-cluster --cluster-id $cluster_id | jq -r .Cluster.Status.State`
      echo $($date_command) Cluster $cluster_id state is $cluster_state
      [ $cluster_state != TERMINATING ] && echo Wait $seconds_to_wait seconds...; sleep $seconds_to_wait
    done

  else
    echo "Adding steps to existing EMR cluster $cluster_id"
    addStepsToCluster

    seconds_to_wait=30
    for step in "${step_ids[@]}"; do
      echo "Waiting for Step $step to complete."
      step_status=$(aws emr describe-step --cluster-id ${cluster_id} --step-id ${step} | jq -r .Step.Status.State)
      while [ "$step_status" != "COMPLETED" ]; do
        step_status=$(aws emr describe-step --cluster-id ${cluster_id} --step-id ${step} | jq -r .Step.Status.State)
        echo "Wait $seconds_to_wait seconds for Step $step to complete..."; sleep $seconds_to_wait
      done
      echo "Step $step has completed."
    done
  fi

  cleanupS3
fi

# Restore provisioned DynamoDB IOPS
[ $new_read_iops -gt $read_capacity ] && echo $($date_command) Restore IOPS on $table from $new_read_iops to $read_capacity \($`echo "$read_capacity * $DYNAMODB_READ_UNIT_PRICE" | bc` per hour\)
