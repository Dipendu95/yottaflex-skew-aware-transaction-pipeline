import hashlib
import json

from pyspark.sql import functions as F

from src.config import PROCESSED_DIR, REJECTED_DIR


def file_checksum(file_path):
    """Calculate SHA-256 fingerprint of a source file."""

    sha = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            sha.update(chunk)

    return sha.hexdigest()


def publish_run_idempotently(
    spark,
    run_id,
    source_file,
    accepted_df,
    quarantine_df
):
    processed_root = PROCESSED_DIR / "runs"
    rejected_root = REJECTED_DIR / "runs"

    run_processed = processed_root / run_id
    run_rejected = rejected_root / run_id

    accepted_path = run_processed / "accepted"
    quarantine_path = run_rejected / "quarantine"
    manifest_path = run_processed / "manifest.json"

    checksum = file_checksum(source_file)

    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            existing_manifest = json.load(f)

        if existing_manifest["source_checksum"] != checksum:
            raise ValueError(
                "Run ID already exists with different source content. "
                "Use a new run_id."
            )

        print("IDEMPOTENT SKIP: Run already committed.")
        return existing_manifest

    accepted_path.parent.mkdir(
        parents=True, exist_ok=True
    )
    quarantine_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    accepted_df.write.mode("overwrite").parquet(
        str(accepted_path)
    )

    quarantine_df.write.mode("overwrite").parquet(
        str(quarantine_path)
    )

    accepted_readback = spark.read.parquet(
        str(accepted_path)
    )
    quarantine_readback = spark.read.parquet(
        str(quarantine_path)
    )

    accepted_count = accepted_readback.count()
    rejected_count = quarantine_readback.count()

    if (
        accepted_count != accepted_df.count()
        or rejected_count != quarantine_df.count()
    ):
        raise RuntimeError("Output verification failed.")

    manifest = {
        "run_id": run_id,
        "source_checksum": checksum,
        "accepted_path": str(accepted_path),
        "quarantine_path": str(quarantine_path),
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "total_accounted": accepted_count + rejected_count,
        "status": "COMMITTED"
    }

    # Manifest is written last as the commit marker.
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)

    print("RUN COMMITTED:", run_id)

    return manifest


def deduplicate_against_committed_runs(
    spark,
    new_accepted_df,
    current_run_id
):
    processed_root = PROCESSED_DIR / "runs"

    manifests = list(
        processed_root.glob("*/manifest.json")
    )

    previous_paths = []

    for manifest_path in manifests:
        with open(
            manifest_path, "r", encoding="utf-8"
        ) as f:
            manifest = json.load(f)

        if (
            manifest["status"] == "COMMITTED"
            and manifest["run_id"] != current_run_id
            and manifest["accepted_count"] > 0
        ):
            previous_paths.append(
                manifest["accepted_path"]
            )

    if not previous_paths:
        empty_duplicates = (
            new_accepted_df.limit(0)
            .withColumn(
                "reject_reason",
                F.lit(None).cast("string")
            )
            .withColumn(
                "rejection_type",
                F.lit(None).cast("string")
            )
            .withColumn(
                "source_line_number",
                F.lit(None).cast("integer")
            )
            .withColumn(
                "raw_record",
                F.lit(None).cast("string")
            )
        )

        return new_accepted_df, empty_duplicates

    existing_ids = (
        spark.read.parquet(*previous_paths)
        .select("transaction_id")
        .distinct()
    )

    duplicate_rows = (
        new_accepted_df
        .join(
            existing_ids,
            on="transaction_id",
            how="inner"
        )
        .withColumn(
            "reject_reason",
            F.lit("DUPLICATE_ACROSS_RUNS")
        )
        .withColumn(
            "rejection_type",
            F.lit("BUSINESS_VALIDATION")
        )
        .withColumn(
            "source_line_number",
            F.lit(None).cast("integer")
        )
        .withColumn(
            "raw_record",
            F.lit(None).cast("string")
        )
        .withColumn(
            "run_id",
            F.lit(current_run_id)
        )
    )

    new_rows = (
        new_accepted_df
        .join(
            existing_ids,
            on="transaction_id",
            how="left_anti"
        )
        .withColumn(
            "run_id",
            F.lit(current_run_id)
        )
    )

    return new_rows, duplicate_rows
