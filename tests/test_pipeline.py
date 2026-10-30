
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.schemas import curated_transaction_schema, merchant_schema
from src.ingestion import ingest_transactions, EXPECTED_COLUMNS
from src.preflight import preflight_csv
from src.validation import validate_transactions
from src.metrics import save_run_metrics
import src.metrics as metrics_module
import src.publishing as publishing_module


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("YottaFlex_Functional_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_structural_csv_rejection(tmp_path):
    source = tmp_path / "malformed.csv"

    source.write_text(
        ",".join(EXPECTED_COLUMNS) + "\n"
        "TX1,MER1,C1,100,UPI,SUCCESS,2026-09-01T10:00:00Z\n"
        "TX2,MER1,C2,200,UPI,SUCCESS\n",
        encoding="utf-8"
    )

    clean_file, rejects, valid_count = preflight_csv(source)

    assert clean_file.exists()
    assert valid_count == 1
    assert len(rejects) == 1
    assert rejects[0]["source_line_number"] == 3
    assert rejects[0]["raw_record"] != ""


def test_ingestion_and_business_validation(spark, tmp_path):
    source = tmp_path / "transactions.csv"

    rows = [
        ["TX1", "MER1", "C1", "100", "UPI", "SUCCESS",
         "2026-09-01T10:00:00Z"],
        ["TX1", "MER1", "C2", "150", "UPI", "SUCCESS",
         "2026-09-01T11:00:00Z"],
        ["TX2", "UNKNOWN", "C3", "200", "CASH", "SUCCESS",
         "2026-09-01T12:00:00Z"],
        ["TX3", "MER1", "C4", "-5", "UPI", "SUCCESS",
         "2026-09-01T13:00:00Z"]
    ]

    with open(source, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_COLUMNS)
        writer.writerows(rows)

    clean_file, structural_rejects, valid_count = preflight_csv(source)

    assert valid_count == 4
    assert len(structural_rejects) == 0

    raw_df = ingest_transactions(
        spark, str(clean_file), run_id="TEST_VALIDATION"
    )

    merchants = spark.createDataFrame(
        [("MER1", "Test Merchant", "STANDARD")],
        schema=merchant_schema
    )

    accepted, rejected = validate_transactions(raw_df, merchants)

    assert accepted.count() == 1
    assert rejected.count() == 3

    reasons = {
        row["reject_reason"]
        for row in rejected.select("reject_reason").collect()
    }

    assert "UNKNOWN_MERCHANT" in reasons
    assert "NON_POSITIVE_AMOUNT" in reasons
    assert "DUPLICATE_TRANSACTION_ID" in reasons


def test_cross_run_deduplication_and_idempotent_publish(
    spark, tmp_path, monkeypatch
):
    processed = tmp_path / "processed"
    rejected = tmp_path / "rejected"

    monkeypatch.setattr(publishing_module, "PROCESSED_DIR", processed)
    monkeypatch.setattr(publishing_module, "REJECTED_DIR", rejected)

    source = tmp_path / "source.csv"
    source.write_text("test source", encoding="utf-8")

    first_run = spark.createDataFrame(
        [
            ("TX1", "MER1", "C1", Decimal("100.00"),
             "UPI", "SUCCESS", datetime(2026, 9, 1, 10, 0),
             "RUN_A", "source.csv")
        ],
        schema=curated_transaction_schema
    )

    empty_quarantine = first_run.limit(0)

    manifest = publishing_module.publish_run_idempotently(
        spark, "RUN_A", str(source), first_run, empty_quarantine
    )

    assert manifest["status"] == "COMMITTED"
    assert manifest["accepted_count"] == 1

    second_run = spark.createDataFrame(
        [
            ("TX1", "MER1", "C1", Decimal("100.00"),
             "UPI", "SUCCESS", datetime(2026, 9, 1, 10, 0),
             "RUN_B", "source.csv"),
            ("TX2", "MER1", "C2", Decimal("200.00"),
             "CASH", "SUCCESS", datetime(2026, 9, 1, 11, 0),
             "RUN_B", "source.csv")
        ],
        schema=curated_transaction_schema
    )

    new_rows, duplicate_rows = (
        publishing_module.deduplicate_against_committed_runs(
            spark, second_run, "RUN_B"
        )
    )

    assert new_rows.count() == 1
    assert duplicate_rows.count() == 1
    assert duplicate_rows.first()["transaction_id"] == "TX1"

    retry_manifest = publishing_module.publish_run_idempotently(
        spark, "RUN_A", str(source), first_run, empty_quarantine
    )

    assert retry_manifest["accepted_count"] == 1
    assert retry_manifest["source_checksum"] == manifest["source_checksum"]


def test_metrics_reconciliation(tmp_path, monkeypatch):
    monkeypatch.setattr(metrics_module, "LOG_DIR", tmp_path)

    metrics = save_run_metrics(
        run_id="TEST_METRICS",
        source_file="test.csv",
        input_count=10,
        structurally_valid_count=9,
        structural_rejected_count=1,
        accepted_count=7,
        business_rejected_count=2,
        start_time=datetime.now(timezone.utc)
    )

    assert metrics["run_status"] == "SUCCESS"
    assert metrics["accounted_count"] == 10
    assert metrics["reconciliation_passed"] is True

    saved_file = tmp_path / "TEST_METRICS_metrics.json"
    assert saved_file.exists()

    saved_metrics = json.loads(
        saved_file.read_text(encoding="utf-8")
    )

    assert saved_metrics["input_count"] == 10
    assert saved_metrics["accepted_count"] == 7
