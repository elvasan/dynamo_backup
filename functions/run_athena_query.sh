#!/bin/bash

function run_athena_query() {
  table_state=RUNNING
  counter=0
  sleep_time=5

  # To handle any transient Athena errors, attempt the query up to 3 times.
  while [ SUCCEEDED != $table_state ] && [ $counter -lt 3 ]; do

      query_id=`aws athena start-query-execution --query-string "$1" --query-execution-context "Database=$2" --result-configuration OutputLocation="s3://aws-athena-query-results-298785453590-us-east-1" --query 'QueryExecutionId' --output text`
      table_state=`aws athena get-query-execution --query-execution-id $query_id --query 'QueryExecution.Status.State' --output text`

      echo `date` Query $query_id state is $table_state
      while [ RUNNING == $table_state ] || [ SUBMITTED == $table_state ]; do
          echo `date` "still running, wait another $sleep_time seconds..."
          sleep $sleep_time
          table_state=`aws athena get-query-execution --query-execution-id $query_id --query 'QueryExecution.Status.State' --output text`
      done
      counter=$(( $counter + 1 ))
      [ SUCCEEDED != $table_state ] && echo `date` "Retrying query. Retry $counter of 3."
  done

  # [ SUCCEEDED != $table_state ] && exit 1 || exit 0

}
