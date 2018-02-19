#!/bin/bash

# get_latest_s3_key
#
# Returns the most recently written S3 key given a path. The keys must be
# formatted YYYY-MM-DD-HH-MM-SS.
# $1->bucket path

function get_latest_s3_key() {
  latest_millis=0
  latest_key=""

  # Set date command to use based on OS. Use gdate on Macs, date elsewhere.
  [[ $OSTYPE =~ 'darwin' ]] && date_command='gdate' || date_command='date'

  for key in $(aws s3 ls ${1} | awk '{ print $2 }' | cut -d / -f 1); do
    this_date=$(echo ${key} | awk -F "-" '{ printf "%s-%s-%s %s:%s:%s", $1, $2, $3, $4, $5, $6 }')
    this_millis=$(${date_command} -d "${this_date}" +%s)

    if [[ ${this_millis} -gt ${latest_millis} ]]; then
      latest_millis=${this_millis}
      latest_key=${key}
    fi
  done

  echo ${latest_key}

}
