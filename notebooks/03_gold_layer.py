import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Account Transaction Joined with Customer #####


@dlt.table(
    name = 'gold_mv_accounts_transactions_joined_with_customers',
    comment = 'Joined account transaction with customer to create MV'
)

def gold_mv_accounts_transactions_joined_with_customers():
    cusomters = dlt.read('scd1_silver_customers_transformed')
    accounts_transactions = dlt.read('scd2_silver_accounts_transactions_transformed')

    joined = cusomters.join(accounts_transactions, accounts_transactions.cust_id == cusomters.customer_id, "inner")
    
    return joined


##### Banking Customer Transaction Report #####

@dlt.table(
    name = 'gold_mv_banking_customer_transaction_report',
    comment = 'Final Banking Customer Transaction Report'
)

def gold_mv_banking_customer_transaction_report():
    df = dlt.read('gold_mv_accounts_transactions_joined_with_customers')
    df_agg = df.groupBy(
                        'customer_id', 'name', 'gender', 'city', 'status', 
                        'income_range', 'risk_segment', 'customer_age', 'tenure_days', 'channel_type'
                        ).agg(
                            countDistinct('account_id').alias('total_accounts'),
                            count('*').alias('total_transactions'),
                            round(sum(when(col('txn_type') == 'Credit', col('txn_amount')).otherwise(lit(0.0))),1).alias('total_credit'),
                            round(sum(when(col('txn_type') == 'Debit', col('txn_amount')).otherwise(lit(0.0))),1).alias('total_debit'),
                            round(sum('balance'),1).alias('total_balance')
                            )
    return df_agg
