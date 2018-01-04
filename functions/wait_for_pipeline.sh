#!/bin/bash

function wait_for_pipeline() {
  pipeline_check_seconds_to_wait=60
  activity_finished=0
  data_pipeline_activity=TableBackupActivity
  while [ $activity_finished -eq 0 ]
  do
    activity_finished=`aws datapipeline list-runs --pipeline-id $pipeline_id | grep $data_pipeline_activity | head -n 1 | grep FINISHED | wc -l`
    echo `date` $data_pipeline_activity activity not finished - waiting $pipeline_check_seconds_to_wait seconds...
    [ $activity_finished -eq 0 ] && sleep $pipeline_check_seconds_to_wait
  done
}
