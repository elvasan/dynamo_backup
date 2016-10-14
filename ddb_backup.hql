SET dynamodb.throughput.read.percent=READ_PERCENTAGE;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- DynamoDB source table
CREATE EXTERNAL TABLE IF NOT EXISTS `ddb_TABLENAME` (item map<string, string>)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "TABLENAME");

-- Create native Hive table target destination
CREATE TABLE IF NOT EXISTS `hive_TABLENAME` (item map<string,string>);

-- Read data from DynamoDB and write to native Hive table
INSERT OVERWRITE TABLE `hive_TABLENAME`
SELECT *
FROM ddb_TABLENAME;
