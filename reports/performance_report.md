# Performance Benchmark Report

## 1. Environment

- Execution environment: Google Colab
- Spark mode: local[*]
- PySpark version: 3.5.6
- Shuffle partitions: 8
- Adaptive Query Execution: Disabled
- Automatic broadcast join threshold: Disabled for join experiment
- Input data: 1,000,000 transactions
- Benchmark methodology: One warm-up followed by three measured runs
- Input DataFrames were cached before benchmarking.

## 2. Data Skew

The top five merchants account for 59.84% of transaction records.

This creates an uneven distribution when partitioning by merchant_id.

## 3. Aggregation Benchmark

| Dataset | Baseline | Salted | Result |
|---|---:|---:|---|
| 100K transactions | 0.147 s | 0.243 s | Salted 65.3% slower |
| 1M transactions | 0.240 s | 0.291 s | Salted 21.3% slower |

## 4. Skewed Join Benchmark

| Run | Baseline | Salted |
|---|---:|---:|
| 1 | 1.205 s | 1.820 s |
| 2 | 1.298 s | 1.737 s |
| 3 | 1.260 s | 3.298 s |
| Average | 1.254 s | 2.285 s |

Both approaches produced 20,000,000 joined records.

The salted join was approximately 82.1% slower.

## 5. Partition Distribution

| Metric | Baseline | Salted |
|---|---:|---:|
| Transaction rows | 1,000,000 | 1,000,000 |
| Maximum partition rows | 417,650 | 175,060 |
| Minimum partition rows | 43,640 | 98,617 |
| Maximum/minimum ratio | 9.57 | 1.78 |

Salting reduced the transaction-side partition imbalance ratio by approximately 81.4%.

The event-side distribution was already relatively balanced.

## 6. Correctness

- Baseline joined records: 20,000,000
- Salted joined records: 20,000,000
- Baseline aggregate groups: 20,000
- Salted aggregate groups: 20,000
- Aggregate differences: 0

The salted join preserved the verified merchant-event aggregate results.

## 7. Physical Plan Observations

Both join strategies used SortMergeJoin.

The baseline shuffled using merchant_id.

The salted strategy shuffled using merchant_id and salt, and introduced:
- Salt calculation
- Event-row replication
- Additional key processing
- Additional sorting and shuffle overhead

## 8. Conclusion

Salting improved transaction-side partition balance but did not improve
measured execution time in this local benchmark.

The experiment demonstrates that better partition distribution
does not automatically translate into faster execution.

Spark's existing partial processing and the relatively small benchmark
may reduce the benefit of salting.

## 9. Limitations

- Benchmarks ran on a single Colab machine, not a distributed cluster.
- Only three measured runs were used after warm-up.
- The join benchmark had a noticeable runtime outlier.
- Partition counts are distribution proxies, not actual task-duration metrics.
- Results may differ on a larger distributed Spark cluster.