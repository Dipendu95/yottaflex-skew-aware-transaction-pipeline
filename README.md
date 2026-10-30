# Skew-Aware Transaction Processing Pipeline Using PySpark

## Project Overview

This project implements a transaction-processing pipeline using PySpark, focusing on data skew diagnosis, salting, transaction validation, reliable publishing, and run-level observability.

**Technology Stack:** Python, PySpark, Pandas, NumPy, Pytest

## Key Objectives

- Identify data skew in transaction workloads.
- Compare baseline and salted aggregation and join strategies.
- Validate transactions against explicit schemas and business rules.
- Quarantine malformed and invalid records.
- Detect duplicate transactions within and across committed runs.
- Support checksum-based idempotent publishing.
- Maintain run-level metrics and reconciliation.

## Architecture

```text
Raw Transaction CSV
        |
        v
Structural Preflight Validation
        |
        +----------------------+
        |                      |
        v                      v
Clean Records          Structural Rejects
        |
        v
Spark Ingestion
        |
        v
Schema & Business Validation <--- Merchant Reference CSV
        |
        +----------------------+
        |                      |
        v                      v
Accepted Records       Business Rejects
        |
        v
Cross-Run Deduplication
        |
        v
Idempotent Publishing
        |
        +----------------------+
        |                      |
        v                      v
Curated Parquet       Quarantine Parquet
        |
        v
Run Metrics & Reconciliation
```

## Dataset

The project uses a generated transaction dataset with the following characteristics:

| Attribute | Value |
|---|---:|
| Transactions | 100,000 |
| Merchants | 1,000 |
| Customers | 20,000 |
| Hot merchants | 5 |
| Hot merchant transaction share | 59.84% |
| Transaction date range | January–September 2026 |

Five merchants account for approximately 60% of transactions, intentionally creating a skewed distribution.

## Data Validation

### Structural validation
- Missing or extra CSV fields
- Malformed CSV records
- Invalid quoting
- Structural record quarantine

### Business validation
- Missing transaction, merchant, or customer IDs
- Invalid or missing transaction amounts
- Non-positive transaction amounts
- Invalid payment methods or transaction statuses
- Invalid or missing timestamps
- Unknown merchant references
- Duplicate transaction IDs

Rejected records retain their rejection reasons.

## Skew Detection and Mitigation

### Aggregation
For hot merchants, a deterministic salt is generated using the transaction ID and distributed across eight buckets. A partial aggregation groups by merchant and salt, followed by a second aggregation by merchant.

### Join
The salted join distributes hot-merchant transactions across salt buckets and replicates matching merchant event records across those buckets.

## Performance Results

Benchmarks were conducted in a local Google Colab Spark environment.

### Aggregation benchmark

| Dataset | Baseline average | Salted average |
|---|---:|---:|
| 100K transactions | 0.147 sec | 0.243 sec |
| 1M transactions | 0.240 sec | 0.291 sec |

### Join benchmark

| Metric | Baseline | Salted |
|---|---:|---:|
| Input transactions | 100,000 | 100,000 |
| Merchant events | 20,000 | 20,700 |
| Output rows | 20,000,000 | 20,000,000 |
| Average execution time | 1.254 sec | 2.285 sec |

### Partition distribution

| Workload | Baseline max/min | Salted max/min |
|---|---:|---:|
| Transaction aggregation | 9.57x | 1.80x |
| Transaction-side join | 9.57x | 1.78x |

**Interpretation:** Salting materially improved partition balance but did not improve measured execution time. Its additional processing and shuffle overhead outweighed the balancing benefit in this local environment. Performance optimization must be benchmarked rather than assumed.

See `reports/performance_report.md` for further details.

## Reliability and Idempotency

- Run-specific output directories.
- SHA-256 source-file checksums.
- Manifest-based commit markers.
- Retry detection for previously committed runs.
- Cross-run transaction deduplication.
- Accepted/rejected record reconciliation.
- JSON run metrics.

The manifest is written after output datasets and acts as the commit marker.

**Limitation:** This implementation is designed for a single-process local demonstration. Manifest publishing is not a fully transactional or concurrency-safe distributed commit protocol.

## Testing

Run the functional tests from the project root:

```bash
pytest -v tests/test_pipeline.py
```

The current test suite covers:
1. Structural CSV rejection.
2. Ingestion and business validation.
3. Cross-run deduplication and idempotent publishing.
4. Run-metric reconciliation.

Separate notebook experiments cover larger-scale benchmarking and integration validation.

## Setup and Execution

### Prerequisites
- Python 3
- Java 17
- Apache Spark through PySpark

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run tests

```bash
pytest -v tests/test_pipeline.py
```

### Run the pipeline

```bash
python -m src.main
```

The default input files are configured in `src/config.py`.

## Project Structure

```text
yottaflex_project/
|-- data/
|   |-- raw/
|   |-- processed/
|   |-- rejected/
|   |-- sample/
|-- src/
|   |-- config.py
|   |-- schemas.py
|   |-- spark_session.py
|   |-- ingestion.py
|   |-- preflight.py
|   |-- validation.py
|   |-- publishing.py
|   |-- metrics.py
|   |-- main.py
|-- tests/
|   |-- test_pipeline.py
|-- reports/
|   |-- performance_report.md
|-- docs/
|-- logs/
|-- requirements.txt
|-- README.md
```

## Limitations and Future Improvements

- Benchmark results are specific to a local Colab environment.
- The benchmark uses a limited number of measured repetitions.
- Partition row counts indicate balance, not actual task duration.
- CSV preflight assumes one physical line per record; multiline quoted fields are unsupported.
- Publishing is not designed for concurrent writers.
- A distributed deployment could evaluate adaptive query execution, executor-level metrics, and transactional storage.

## Conclusion

This project demonstrates a practical PySpark transaction pipeline with skew analysis, salting experiments, data-quality controls, cross-run deduplication, idempotent publishing, observability, and functional testing.

The key finding is that salting improved partition balance but did not reduce runtime in the tested local workloads.
