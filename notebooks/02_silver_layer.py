import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Customer Files Transformed #####

@dlt.table(
    name = 'silver_customers_transformed',
    comment = 'Customers Transformed'
)


def silver_customers_transformed():
    df = dlt.read_stream('bronze_customers_cleansed')
    df = df.withColumn('customer_age', floor(months_between(current_date(), col('dob'))/12))
    df = df.withColumn('tenure_days', datediff(current_date(), col('join_date')))
    df = df.withColumn('dob_out_of_range', (col('dob') < lit('1900-01-01')) | (col('dob') > current_date()))
    df = df.withColumn('transformation_date', current_timestamp())

    df = df.filter(col('dob_out_of_range').isin(False))
    
    return df


##### Account Transaction Files Transformed #####

@dlt.table(
    name = 'silver_accounts_transactions_transformed',
    comment = 'Account Transactions Transformed'
)


def silver_accounts_transactions_transformed():
    df = dlt.read_stream('bronze_accounts_transactions_cleansed')
    df = df.withColumn('channel_type', when(col('txn_channel').isin(['APP', 'ONLINE', 'MOBILE']), "DIGITAL").when(col('txn_channel').isin(['BRANCH', 'ATM']), "PHYSICAL").otherwise(lit("UNKNOWN")))
    df = df.withColumn('txn_year',year(col('txn_date')))
    df = df.withColumn('txn_month',month(col('txn_date')))
    df = df.withColumn('txn_dayofmonth',dayofmonth(col('txn_date')))
    df = df.withColumn('is_weekend', when(dayofweek(col('txn_date')).isin([1,7]), lit('Weekend')).otherwise(lit('Weekday')))
    df = df.withColumn('txn_dayofweek', date_format(col('txn_date'), 'EEEE'))
    df = df.withColumn('txn_quarter', quarter(col('txn_date')))
    df = df.withColumn('txn_yearqtr', concat(year(col('txn_date')), lit('-Q'), quarter(col('txn_date'))))
    df = df.withColumn('transformation_date', current_timestamp())

    return df