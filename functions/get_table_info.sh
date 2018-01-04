#!/bin/bash

ceiling() {
  echo "define ceil (x) {if (x<0) {return x/1} \
        else {if (scale(x)==0) {return x} \
        else {return x/1 + 1 }}} ; ceil($1)" | bc
}

round() {
  echo $(printf %.$2f $(echo "scale=$2;(((10^$2)*$1)+0.5)/(10^$2)" | bc))
};

get_capacity_partitions() {
  tableSizeInGB=$(echo $table_size/1024/1024/1024 | bc)
  capacityPartitions=$(ceiling $tableSizeInGB/10)
}

get_throughput_partitions() {
  throughputPartitions=$(echo $read_capacity / 3000 + $write_capacity / 1000 | bc)
  throughputPartitions=$(round $throughputPartitions 0)

  if [ $throughputPartitions -lt 1 ]; then
    throughputPartitions=1
  fi
}

get_actual_partitions() {
  get_capacity_partitions
  get_throughput_partitions

  if [ $capacityPartitions -gt $throughputPartitions ]; then
    actualPartitions=$capacityPartitions
  else
    actualPartitions=$throughputPartitions
  fi
}

get_complete_time_throughput() {

  completeTimeThroughput=$(echo $table_size/3600/8192 | bc)

  if [ $completeTimeThroughput -eq 0 ]; then
    completeTimeThroughput=1
  fi
}

get_maximum_read_throughput() {
  let "maximumReadThroughput = 3000 * $actualPartitions"
}

get_new_read_iops() {
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

print_table_info() {
  echo -e "Table:\t\t\t $table_name"
  echo -e "Current Read Capacity:\t $read_capacity"
  echo -e "Current Write Capacity:\t $write_capacity"
  echo -e "Item Count:\t\t $item_count"
  echo -e "Size (bytes):\t\t $table_size"
  echo -e "Estimated Partitions:\t $actualPartitions"
  echo -e "Maximum Read IOPS:\t $maximumReadThroughput"
  echo -e "Complete Time IOPS:\t $completeTimeThroughput"
  echo ""
}

function get_table_info() {
  describe_table=`aws dynamodb describe-table --table-name $1`
  read_capacity=`echo $describe_table | jq '.Table.ProvisionedThroughput.ReadCapacityUnits'`
  write_capacity=`echo $describe_table | jq '.Table.ProvisionedThroughput.WriteCapacityUnits'`
  table_size=`echo $describe_table | jq '.Table.TableSizeBytes'`
  item_count=`echo $describe_table | jq '.Table.ItemCount'`

  get_actual_partitions
  get_complete_time_throughput
  get_maximum_read_throughput
  get_new_read_iops

  print_table_info
}
