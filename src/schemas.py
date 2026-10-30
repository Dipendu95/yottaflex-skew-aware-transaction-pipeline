from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType
)

# Raw input: retain values as strings for validation.
raw_transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_amount", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# Curated transaction schema.
curated_transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_amount", DecimalType(12, 2), False),
    StructField("payment_method", StringType(), False),
    StructField("transaction_status", StringType(), False),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("run_id", StringType(), False),
    StructField("source_file", StringType(), False)
])

# Merchant reference schema.
merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("merchant_tier", StringType(), False)
])
