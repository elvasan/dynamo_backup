SET dynamodb.throughput.read.percent=READ_PERCENTAGE;
SET mapred.map.tasks.speculative.execution=false;
SET mapred.reduce.tasks.speculative.execution=false;
SET hive.mapred.reduce.tasks.speculative.execution=false;
SET hive.default.fileformat=Orc;
SET hive.exec.orc.default.compress=SNAPPY;
SET hive.vectorized.execution.enabled=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.cbo.enable=true;
SET hive.merge.mapfiles=true;

-- DynamoDB source table
CREATE EXTERNAL TABLE IF NOT EXISTS `ddb_TABLENAME` (item map<string, string>)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "TABLENAME");

-- Read data from DynamoDB and write to native Hive table
CREATE TABLE `hive_TABLENAME` AS SELECT item,
  CASE
    WHEN CAST(GET_JSON_OBJECT(item["capture_time"], "$.n") AS BIGINT) >= (1000 * UNIX_TIMESTAMP('THIS_MONTH', 'yyyy-MM-dd'))
      AND CAST(GET_JSON_OBJECT(item["capture_time"], "$.n") AS BIGINT) < (1000 * UNIX_TIMESTAMP('NEXT_MONTH', 'yyyy-MM-dd'))
  THEN TO_DATE(FROM_UNIXTIME(CAST(CAST(GET_JSON_OBJECT(item["capture_time"], "$.n") AS BIGINT) / 1000 AS BIGINT)))
  ELSE TO_DATE('1970-01-01')
END created FROM ddb_TABLENAME;
