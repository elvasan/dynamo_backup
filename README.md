# DynamoDB Backup to S3

This script automates the backup of an AWS DynamoDB table to S3. This is done
by provisioning an Elastic Map Reduce (EMR) cluster to read the data from DynamoDB,
raising read throughput to do, and then writing the data to S3.

## Usage

```
./backup_dynamodb_table_to_s3 -t table_name [-d MMYY -c seconds]
    -t: DynamoDB table name to backup
    -d: (optional) Date suffix to use for monthly sharded tables. If not set, current month and year will be used.
    -c: (optional) Time to complete, in seconds (default: 3600)
    -b: (optional) Spot bid price, in dollars (default: \$2.00)
    -i: (optional) EMR core instance type (default: d2.8xlarge)
    -a: (optional) Availability Zone to be used for EMR cluster (default: us-east-1e)
    -x: (optional) Enable debug mode. EMR cluster will not provisioned but throughput info will be printed.
```

## Dependencies

To run this script locally, the user will require AWS IAM permissions for the following items:

* Create and use an EMR cluster.
* Write and delete data from the configured S3 backup location.

Additionally, when run on Mac OS, the script requires the `gdate`
utility. This tool can be installed via Homebrew in the `coreutils` package.

## Internal Constant Values

The script uses some constants to control its calculations and execution. These are stated in the below table.

| Name | Description | Value |
| ---- | ----------- | ----- |
| BACKUP_HQL | The name of the template HQL script to be used for reading data from DynamoDB. The backup script will replace values from this file to generate customized backup query for the requested table. | `ddb_backup.hql` |
| S3_WRITE_HQL | The name of the template HQL script to be used for writing data to S3. The backup script will replace values from this file to generate a customized query for the requested table. | `s3_write.hql` |
| S3_BUCKET | The S3 bucket name where the backup should be written. | `jornaya-backup` |
| READ_PERCENTAGE | The maximum percentage of read IOPS that the EMR cluster should consume when reading data from DynamoDB. | `1.5` |
| DYNAMODB_READ_UNIT_PRICE | The price of one DynamoDB read IOP. Used to display price information on the cost of raising the throughput per hour. | `0.00013` |
| MAX_READ_IOPS | The maximum allowable setting for DynamoDB read IOPS. | `40000` |

## DynamoDB Read Throughput

The script calculates the appropriate value to which to raise DynamoDB read throughput. This calculation makes an estimate of the current table partitions and will not raise the throughput to a level that would cause re-partitioning.

The script will calculate two values for read IOPS:

* __Complete Time Throughput__: the read IOPS setting necessary to completely read the table within one hour.
* __Maximum Throughput__: the maximum read IOPS setting which can be made without causing re-partitioning.

The table's read IOPS will then be set to the lesser of these two values if those values do not exceed a hard-coded absolute maximum read IOPS value (see Internal Constant Values).

Additionally, the table's read throughput will be raised and lowered immediately before and following the execution of the read from DynamoDB to EMR. In this fashion, the throughput is only raised for the necessary time.

## Todo

* Support a test-mode where the read IOPS and scripts will be generated, but an EMR cluster will not be provisioned.
* Support specifying the number of EMR nodes as a command line option.
* Support unsharded tables.
