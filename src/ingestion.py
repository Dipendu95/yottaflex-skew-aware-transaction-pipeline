import csv
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.schemas import raw_transaction_schema


EXPECTED_COLUMNS = [
    "transaction_id",
    "merchant_id",
    "customer_id",
    "transaction_amount",
    "payment_method",
    "transaction_status",
    "transaction_timestamp"
]


def ingest_transactions(spark, file_path, run_id=None):

    file_path = str(file_path)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    with open(file_path, "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        actual_columns = next(reader, [])

    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(
            f"Unexpected source schema. "
            f"Expected {EXPECTED_COLUMNS}, "
            f"received {actual_columns}"
        )

    if run_id is None:
        run_id = str(uuid.uuid4())

    raw_df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
        .schema(raw_transaction_schema)
        .load(file_path)
    )

    raw_df = (
        raw_df
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "source_file",
            F.lit(os.path.basename(file_path))
        )
        .withColumn(
            "ingested_at",
            F.lit(datetime.now(timezone.utc).isoformat())
        )
    )

    return raw_df
