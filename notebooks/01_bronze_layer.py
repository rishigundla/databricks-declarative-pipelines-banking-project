import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Customers File Cleansed #####


@dlt.table(
    name = 'bronze_customers_cleansed',
    comment = 'Customer Cleansed'
)


@dlt.expect_all_or_fail({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_name": "name IS NOT NULL"
})
@dlt.expect('valid_gender', 'gender IS NOT NULL')
@dlt.expect_or_drop('valid_status', 'status IS NOT NULL')
@dlt.expect_or_drop('valid_email', "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'")


def bronze_customers_cleansed():
  
  df = dlt.read_stream('landing_customers_incremental')
  df = df.withColumn('name', initcap(trim('name')))
  df = df.withColumn('gender', when(col('gender') == 'M', 'Male').when(col('gender') == 'F', 'Female').otherwise('Unknown'))
  df = df.withColumn('city', initcap(trim('city')))
  df = df.withColumn('status', 
                     when(
                         (initcap(trim(col('status'))).isNull()) | (initcap(trim(col('status'))) == ''), 'Unknown'
                         ).otherwise(initcap(trim(col('status'))))
                     )
  df = df.withColumn('email', lower(col('email')))
  df = df.withColumn('email',
                        when(
                            lower(col('email')).rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
                            lower(col('email'))
                        ).otherwise(lit(None))
                     )
  df = df.withColumn('phone_number',regexp_replace(col('phone_number'), r'\s+', ''))
  df = df.withColumn('phone_number', 
                     when(
                         col('phone_number').rlike(r'^\+44\d{10}$'), col('phone_number')
                         ).otherwise(lit(None))
                     )
  df = df.withColumn('preferred_channel', 
                     when(
                         (upper(trim(col('preferred_channel'))).isNull()) | (upper(trim(col('preferred_channel'))) == ''), 'Unknown'
                         ).otherwise(upper(trim(col('preferred_channel'))))
                     )
  df = df.withColumn('preferred_channel', when(col('preferred_channel') == 'ONLNE', 'ONLINE').otherwise(col('preferred_channel')))
  df = df.withColumn('occupation', regexp_replace(trim(initcap(col('occupation'))), r'[^A-Za-z]',''))
  df = df.withColumn('occupation', 
                     when(
                         (initcap(trim(col('occupation'))).isNull()) | (initcap(trim(col('occupation'))) == ''), 'Unknown'
                         ).otherwise(initcap(trim(col('occupation'))))
                     )
  df = df.withColumn('income_range', 
                     when(
                         (initcap(trim(col('income_range'))).isNull()) | (initcap(trim(col('income_range'))) == ''), 'Unknown'
                         ).otherwise(initcap(trim(col('income_range'))))
                     )
  df = df.withColumn('risk_segment', 
                     when(
                         (initcap(trim(col('risk_segment'))).isNull()) | (initcap(trim(col('risk_segment'))) == ''), 'Unknown'
                         ).otherwise(initcap(trim(col('risk_segment'))))
                     )
  
  df = df.dropna(subset=['email', 'phone_number'])
  df = df.filter(col('preferred_channel').isin("ATM", "ONLINE", "MOBILE", "BRANCH", "UNKNOWN"))
  df = df.filter(col('risk_segment').isin("Low", "Medium", "High", "Very High", "Unknown"))
  df = df.filter(col('income_range').isin("Low", "Medium", "High", "Very High", "Unknown"))
  df = df.filter(col('status').isin("Active", "Inactive", "Pending", "Unknown"))

  return df  


##### Account Transactions File Cleansed #####

@dlt.table(
    name = 'bronze_accounts_transactions_cleansed',
    comment = 'Account Transactions Cleansed'
)

@dlt.expect_all_or_fail({
    "valid_account_id": "account_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_transaction_id": "txn_id IS NOT NULL"
})
@dlt.expect("valid_account_type", "account_type IS NOT NULL")
@dlt.expect("valid_txn_channel", "txn_channel IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")


def bronze_accounts_transactions_cleansed():
  
  df = dlt.read_stream('landing_accounts_transactions_incremental')
  df = df.withColumn('txn_type', 
                     when(initcap(trim(col('txn_type'))) == 'Debitt','Debit').when(
                         initcap(trim(col('txn_type'))) == 'Crediit','Credit').otherwise(
                                 initcap(trim(col('txn_type')))) 
                    )
  df = df.withColumn('txn_channel', upper(trim(col('txn_channel'))))
  df = df.withColumn('txn_channel', 
                     when(
                         (col('txn_channel').isNull()) | (col('txn_channel') == ''), 'UNKNOWN'
                         ).otherwise(col('txn_channel'))
                     )
  df = df.withColumnRenamed('customer_id', 'cust_id')
  
  return df