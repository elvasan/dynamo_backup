#!/bin/bash

function wait_for_pipeline() {
  pipeline_check_seconds_to_wait=60
  data_pipeline_activity=TableBackupActivity
  while [ true ]; do
    activity_status=`aws datapipeline list-runs --pipeline-id $pipeline_id | grep $data_pipeline_activity | head -n 1 | tr -s ' ' | awk '{print $4}'`

    if [[ ${activity_status} =~ FINISHED|FAILED|CANCELLED ]]; then
      break
    fi

    echo `date` $pipeline_id status ${activity_status} - waiting $pipeline_check_seconds_to_wait seconds
    sleep $pipeline_check_seconds_to_wait
  done

  if [[ ${activity_status} == FINISHED ]]; then
    echo 0
  else
    echo 1
  fi

}
