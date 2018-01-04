#!/bin/bash

function get_new_rcu() {
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
