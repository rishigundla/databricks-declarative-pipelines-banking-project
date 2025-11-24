import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Customers 2023 File Data Ingestion ####


customer_schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('dob', DateType(), True),
    StructField('gender', StringType(), True),
    StructField('city', StringType(), True),
    StructField('join_date', DateType(), True),
    StructField('status', StringType(), True),
    StructField('email', StringType(), True),
    StructField('phone_number', StringType(), True),
    StructField('preferred_channel', StringType(), True),
    StructField('occupation', StringType(), True),
    StructField('income_range', StringType(), True),
    StructField('risk_segment', StringType(), True)
])


@dlt.table(
    name = 'landing_customers_incremental',
    comment = 'Raw data for customers'
)


def landing_customers_incremental():
  return (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .schema(customer_schema)
          .load('/Volumes/bank/banking_demo/bank_lakehouse/customers/'))
 

##### Account Transactions 2023 File Data Ingestion ####


account_schema = StructType([
    StructField('account_id', IntegerType(), True),
    StructField('customer_id', IntegerType(), True),
    StructField('account_type', StringType(), True),
    StructField('balance', DoubleType(), True),
    StructField('txn_id', IntegerType(), True),
    StructField('txn_date', DateType(), True),
    StructField('txn_type', StringType(), True),
    StructField('txn_amount', DoubleType(), True),
    StructField('txn_channel', StringType(), True)
])


@dlt.table(
    name = 'landing_accounts_transactions_incremental',
    comment = 'Raw data for accounts'
)


def landing_accounts_transactions_incremental():
  return (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .schema(account_schema)
          .load('/Volumes/bank/banking_demo/bank_lakehouse/accounts/'))