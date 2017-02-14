SET dynamodb.throughput.read.percent=READ_PERCENTAGE;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2000;

-- S3 target destination
CREATE EXTERNAL TABLE IF NOT EXISTS `s3_TABLENAME` (item map<string,string>)
PARTITIONED BY (created string)
STORED AS ORC
LOCATION 's3://S3_BUCKET/NO_MMYY/'
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE `s3_TABLENAME`
PARTITION (created)
SELECT *, TO_DATE(FROM_UNIXTIME(CAST(GET_JSON_OBJECT(item["created"], "$.n") AS INT))) created
FROM `hive_TABLENAME`
WHERE TO_DATE(FROM_UNIXTIME(CAST(GET_JSON_OBJECT(item["created"], "$.n") AS INT))) >= TO_DATE(START_DATE)
  AND TO_DATE(FROM_UNIXTIME(CAST(GET_JSON_OBJECT(item["created"], "$.n") AS INT))) < TO_DATE(END_DATE);

DROP TABLE `hive_TABLENAME`;
