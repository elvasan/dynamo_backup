#!/bin/bash

function make_dynamodb_rcu_script() {
  change_dynamodb_rcu_script_name=change_dynamodb_rcu_$table_name.sh
  cat > $change_dynamodb_rcu_script_name <<EOF
#!/bin/bash

current_read=\$(aws dynamodb describe-table --table-name $table_name --query 'Table.ProvisionedThroughput.ReadCapacityUnits')
if [ \$current_read -lt \$1 ]; then
  aws dynamodb update-table --table-name $table_name --provisioned-throughput ReadCapacityUnits=\$1,WriteCapacityUnits=$write_capacity
fi

aws dynamodb describe-table --table-name $table_name --query 'Table.ProvisionedThroughput.ReadCapacityUnits'

if [ \$current_read -ge \$1 ]; then
  echo "Not waiting for table to be active!"
else
  seconds_to_wait=5
  table_state=UPDATING
  while [ \$table_state != ACTIVE ]; do
    table_state=\$(aws dynamodb describe-table --table-name $table_name --query 'Table.TableStatus' --output text)
    echo \$(date) Table $table_name state is \$table_state
    [ \$table_state != ACTIVE ] && echo Wait \$seconds_to_wait seconds...; sleep \$seconds_to_wait
  done
fi
EOF
}
