from pyspark.sql import functions as F
from pyspark.sql.window import Window


VALID_PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "CASH"
]

VALID_STATUSES = [
    "SUCCESS",
    "FAILED"
]


def validate_transactions(raw_df, merchant_df):

    typed_df = (
        raw_df
        .withColumn(
            "transaction_amount",
            F.expr(
                "try_cast(transaction_amount AS DECIMAL(12,2))"
            )
        )
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                "transaction_timestamp",
                "yyyy-MM-dd'T'HH:mm:ssX"
            )
        )
    )

    valid_merchants = merchant_df.select(
        "merchant_id"
    ).distinct()

    typed_df = typed_df.join(
        valid_merchants.withColumn(
            "merchant_exists", F.lit(True)
        ),
        on="merchant_id",
        how="left"
    )

    duplicate_window = (
        Window.partitionBy("transaction_id")
        .orderBy(
            F.sha2(
                F.to_json(
                    F.struct(
                        "merchant_id",
                        "customer_id",
                        "transaction_amount",
                        "payment_method",
                        "transaction_status",
                        "transaction_timestamp"
                    )
                ),
                256
            ).asc()
        )
    )

    typed_df = typed_df.withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window)
    )

    reason = F.concat_ws(
        "|",

        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.lit("MALFORMED_RECORD")
        ),

        F.when(
            F.col("transaction_id").isNull() |
            (F.trim("transaction_id") == ""),
            F.lit("MISSING_TRANSACTION_ID")
        ),

        F.when(
            F.col("merchant_id").isNull() |
            (F.trim("merchant_id") == ""),
            F.lit("MISSING_MERCHANT_ID")
        ),

        F.when(
            F.col("customer_id").isNull() |
            (F.trim("customer_id") == ""),
            F.lit("MISSING_CUSTOMER_ID")
        ),

        F.when(
            F.col("transaction_amount").isNull(),
            F.lit("INVALID_OR_MISSING_AMOUNT")
        ),

        F.when(
            F.col("transaction_amount") <= 0,
            F.lit("NON_POSITIVE_AMOUNT")
        ),

        F.when(
            ~F.col("payment_method").isin(
                VALID_PAYMENT_METHODS
            ) |
            F.col("payment_method").isNull(),
            F.lit("INVALID_PAYMENT_METHOD")
        ),

        F.when(
            ~F.col("transaction_status").isin(
                VALID_STATUSES
            ) |
            F.col("transaction_status").isNull(),
            F.lit("INVALID_TRANSACTION_STATUS")
        ),

        F.when(
            F.col("transaction_timestamp").isNull(),
            F.lit("INVALID_OR_MISSING_TIMESTAMP")
        ),

        F.when(
            F.col("merchant_id").isNotNull() &
            F.col("merchant_exists").isNull(),
            F.lit("UNKNOWN_MERCHANT")
        ),

        F.when(
            F.col("transaction_id").isNotNull() &
            (F.trim("transaction_id") != "") &
            (F.col("_duplicate_rank") > 1),
            F.lit("DUPLICATE_TRANSACTION_ID")
        )
    )

    validated_df = typed_df.withColumn(
        "reject_reason", reason
    )

    accepted_df = (
        validated_df
        .filter(F.col("reject_reason") == "")
        .select(
            "transaction_id",
            "merchant_id",
            "customer_id",
            "transaction_amount",
            "payment_method",
            "transaction_status",
            "transaction_timestamp",
            "run_id",
            "source_file"
        )
    )

    rejected_df = (
        validated_df
        .filter(F.col("reject_reason") != "")
        .drop(
            "merchant_exists",
            "_duplicate_rank"
        )
    )

    return accepted_df, rejected_df
