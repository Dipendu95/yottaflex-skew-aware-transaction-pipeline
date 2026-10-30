
from datetime import datetime, timezone
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

from src.config import (
    TRANSACTION_FILE,
    MERCHANT_FILE
)

from src.spark_session import create_spark_session
from src.schemas import merchant_schema

from src.preflight import preflight_csv
from src.ingestion import ingest_transactions
from src.validation import validate_transactions
from src.publishing import (
    publish_run_idempotently,
    deduplicate_against_committed_runs
)
from src.metrics import save_run_metrics


def build_structural_quarantine(spark, rejects, run_id):
    structural_schema = StructType([
        StructField("source_file", StringType(), True),
        StructField("source_line_number", IntegerType(), True),
        StructField("raw_record", StringType(), True),
        StructField("reject_reason", StringType(), True)
    ])

    structural_df = spark.createDataFrame(
        [
            (
                r["source_file"],
                int(r["source_line_number"]),
                r["raw_record"],
                r["reject_reason"]
            )
            for r in rejects
        ],
        schema=structural_schema
    )

    return (
        structural_df
        .withColumn("merchant_id", F.lit(None).cast("string"))
        .withColumn("transaction_id", F.lit(None).cast("string"))
        .withColumn("customer_id", F.lit(None).cast("string"))
        .withColumn(
            "transaction_amount",
            F.lit(None).cast("decimal(12,2)")
        )
        .withColumn("payment_method", F.lit(None).cast("string"))
        .withColumn("transaction_status", F.lit(None).cast("string"))
        .withColumn(
            "transaction_timestamp",
            F.lit(None).cast("timestamp")
        )
        .withColumn("_corrupt_record", F.lit(None).cast("string"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "ingested_at",
            F.current_timestamp().cast("string")
        )
        .withColumn(
            "rejection_type",
            F.lit("STRUCTURAL_VALIDATION")
        )
    )


def run_pipeline(
    transaction_file=TRANSACTION_FILE,
    merchant_file=MERCHANT_FILE,
    run_id=None,
    apply_cross_run_dedup=True
):
    start_time = datetime.now(timezone.utc)
    run_id = run_id or f"YF_{uuid.uuid4().hex[:12].upper()}"

    spark = create_spark_session()

    input_count = 0
    structurally_valid_count = 0
    structural_rejected_count = 0
    accepted_count = 0
    business_rejected_count = 0

    try:
        print(f"Starting pipeline: {run_id}")

        # 1. Structural preflight
        clean_file, structural_rejects, structurally_valid_count = (
            preflight_csv(transaction_file)
        )

        structural_rejected_count = len(structural_rejects)

        input_count = (
            structurally_valid_count + structural_rejected_count
        )

        print("Input records:", input_count)
        print("Structurally valid:", structurally_valid_count)
        print("Structural rejects:", structural_rejected_count)

        # 2. Spark ingestion
        raw_df = ingest_transactions(
            spark,
            str(clean_file),
            run_id=run_id
        )

        # 3. Load merchant reference
        merchant_df = (
            spark.read
            .option("header", True)
            .schema(merchant_schema)
            .csv(str(merchant_file))
        )

        # 4. Business validation
        valid_df, rejected_df = validate_transactions(
            raw_df,
            merchant_df
        )

        validation_rejected_count = rejected_df.count()

        # 5. Build unified quarantine
        business_quarantine = (
            rejected_df
            .withColumn(
                "rejection_type",
                F.lit("BUSINESS_VALIDATION")
            )
            .withColumn(
                "source_line_number",
                F.lit(None).cast(IntegerType())
            )
            .withColumn(
                "raw_record",
                F.col("_corrupt_record")
            )
        )

        structural_quarantine = build_structural_quarantine(
            spark,
            structural_rejects,
            run_id
        )

        unified_quarantine = business_quarantine.unionByName(
            structural_quarantine,
            allowMissingColumns=True
        )

        # 6. Cross-run deduplication
        if apply_cross_run_dedup:
            new_df, duplicate_df = (
                deduplicate_against_committed_runs(
                    spark,
                    valid_df,
                    run_id
                )
            )

            unified_quarantine = unified_quarantine.unionByName(
                duplicate_df,
                allowMissingColumns=True
            )

            accepted_df = new_df
            cross_run_duplicate_count = duplicate_df.count()

        else:
            accepted_df = valid_df
            cross_run_duplicate_count = 0

        accepted_count = accepted_df.count()

        business_rejected_count = (
            validation_rejected_count
            + cross_run_duplicate_count
        )

        quarantine_count = unified_quarantine.count()

        # 7. Reconciliation
        accounted_count = accepted_count + quarantine_count

        print("\n--- PIPELINE RECONCILIATION ---")
        print("Input:", input_count)
        print("Accepted:", accepted_count)
        print("Business rejected:", business_rejected_count)
        print("Structural rejected:", structural_rejected_count)
        print("Total quarantined:", quarantine_count)
        print("Total accounted:", accounted_count)

        if input_count != accounted_count:
            raise ValueError(
                f"Reconciliation failed: input={input_count}, "
                f"accounted={accounted_count}"
            )

        # 8. Publish accepted and quarantined records
        publish_result = publish_run_idempotently(
            spark,
            run_id,
            str(transaction_file),
            accepted_df,
            unified_quarantine
        )

        # 9. Save run metrics
        save_run_metrics(
            run_id=run_id,
            source_file=str(transaction_file),
            input_count=input_count,
            structurally_valid_count=structurally_valid_count,
            structural_rejected_count=structural_rejected_count,
            accepted_count=accepted_count,
            business_rejected_count=business_rejected_count,
            start_time=start_time,
            run_status="SUCCESS"
        )

        print("\nPIPELINE SUCCESS:", run_id)

        return {
            "run_id": run_id,
            "input_count": input_count,
            "accepted_count": accepted_count,
            "business_rejected_count": business_rejected_count,
            "structural_rejected_count": structural_rejected_count,
            "quarantine_count": quarantine_count,
            "publish_result": publish_result
        }

    except Exception as exc:
        try:
            save_run_metrics(
                run_id=run_id,
                source_file=str(transaction_file),
                input_count=input_count,
                structurally_valid_count=structurally_valid_count,
                structural_rejected_count=structural_rejected_count,
                accepted_count=accepted_count,
                business_rejected_count=business_rejected_count,
                start_time=start_time,
                run_status="FAILED",
                failure_reason=str(exc)
            )
        except Exception as metrics_exc:
            print("Could not save failure metrics:", metrics_exc)

        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
