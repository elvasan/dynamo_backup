SET dynamodb.throughput.read.percent=READ_PERCENTAGE;

-- S3 target destination
CREATE EXTERNAL TABLE IF NOT EXISTS `s3_TABLENAME` (item map<string,string>)
STORED AS ORC
LOCATION 's3://S3_BUCKET/NO_MMYY/'
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE `s3_TABLENAME`
SELECT *
FROM `hive_TABLENAME`;

DROP TABLE `hive_TABLENAME`;
