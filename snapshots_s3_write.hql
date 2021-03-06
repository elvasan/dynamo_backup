SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- S3 target destination
CREATE EXTERNAL TABLE IF NOT EXISTS `s3_TABLENAME` (item map<string,string>)
PARTITIONED BY (created string)
STORED AS PARQUET
LOCATION 's3://S3_BUCKET/NO_MMYY/TABLENAME/'
TBLPROPERTIES ("PARQUET.COMPRESS"="SNAPPY");

INSERT OVERWRITE TABLE `s3_TABLENAME`
PARTITION (created)
SELECT * FROM `hive_TABLENAME`;
