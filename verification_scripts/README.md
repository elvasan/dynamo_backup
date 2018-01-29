# DynamoDB Backup Tests

This scripts verifies the successful backup of sharded DynamoDB tables to S3.

## Dependencies

Installing Pipenv allows for a seamless means to manage virtual environments and
packages that are necessary to run the project.

### Installation

```
pip install pipenv
```
or
```
brew install pipenv
```

## Usage

From the `verification_scripts` directory, running the following command will install all of
the necessary packages.

```
pipenv install
```
Then active virtual environment shell with the following command

```
pipenv run python dynamodb_backup_test.py --table table_name

```
