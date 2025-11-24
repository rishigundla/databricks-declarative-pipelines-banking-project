import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Customer Files Transformed - SCD1 with Apply Changes #####

dlt.create_streaming_table("scd1_silver_customers_transformed")
dlt.apply_changes(
    target='scd1_silver_customers_transformed',
    source='silver_customers_transformed',
    keys=['customer_id'],
    sequence_by=col('transformation_date'),
    stored_as_scd_type=1,
    except_column_list=['transformation_date']
)


##### Account Transaction Files Transformed - SCD2 with Auto CDC #####

dlt.create_streaming_table("scd2_silver_accounts_transactions_transformed")
dlt.create_auto_cdc_flow(
    target='scd2_silver_accounts_transactions_transformed',
    source='silver_accounts_transactions_transformed',
    keys=['txn_id'],
    sequence_by=col('transformation_date'),
    stored_as_scd_type=2,
    except_column_list=['transformation_date']
)