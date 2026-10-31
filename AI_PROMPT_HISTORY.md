# ChatGPT Prompt History and AI Usage Disclosure

**Project:** Skew-Aware Transaction Processing Pipeline Using PySpark  
**AI tool used:** ChatGPT  
**Other AI tools:** None

## 1. Purpose of AI assistance

ChatGPT was used as an implementation and learning assistant throughout the development of this project. Assistance covered project design, PySpark implementation, debugging, testing, performance analysis, documentation, and GitHub preparation.

The following is a chronological reconstruction of the main prompt topics from the available project conversation records. It is a summary of the requests, not a verbatim export of every prompt.

## 2. Prompt history

| Stage | Prompt/request topic | Purpose |
|---|---|---|
| 1. Project design | Help design a practical skew-aware transaction-processing pipeline using PySpark. | Define project scope, architecture, and technical objectives. |
| 2. Dataset | Help generate a realistic transaction dataset with intentionally skewed merchant distribution. | Create reproducible data for skew experiments. |
| 3. Spark setup | Help configure the PySpark environment and organize the project into reusable Python modules. | Establish a maintainable project structure. |
| 4. Schema and ingestion | Help implement explicit schemas, CSV ingestion, and structural preflight validation. | Handle input data reliably. |
| 5. Data validation | Help implement business-rule validation, duplicate detection, accepted records, and rejected-record quarantine. | Protect data quality. |
| 6. Skew diagnosis | Explain how to identify data skew and implement salted aggregation in PySpark. | Compare baseline and skew-aware processing. |
| 7. Skew-aware joins | Help implement and benchmark a salted join against a baseline join. | Evaluate partition distribution and execution cost. |
| 8. Idempotency | Help implement checksum-based publishing, manifests, retry handling, and cross-run deduplication. | Improve reliability and recovery behavior. |
| 9. Observability | Help implement run-level metrics and input/output reconciliation. | Track pipeline execution and failures. |
| 10. Automated testing | Help write and debug pytest tests for structural validation, business validation, publishing, deduplication, and metrics. | Verify key pipeline behavior. |
| 11. Performance analysis | Help interpret benchmark timings, partition distributions, and physical execution plans. | Understand the trade-offs of salting. |
| 12. Documentation | Help prepare the README, data dictionary, performance report, and project limitations. | Make the project understandable and reproducible. |
| 13. GitHub preparation | Help organize the repository, configure Git, authenticate, and publish the project. | Prepare the public submission repository. |

## 3. How ChatGPT accelerated the work

ChatGPT helped accelerate:
- Designing the modular project architecture.
- Developing and debugging PySpark transformations.
- Structuring validation and quarantine logic.
- Implementing idempotent publishing and cross-run deduplication.
- Writing automated tests and technical documentation.
- Interpreting benchmark results and identifying implementation trade-offs.

## 4. Where AI assistance was substantial

ChatGPT provided substantial implementation guidance and code assistance for several Python modules, including ingestion, validation, preflight processing, publishing, metrics, and pipeline orchestration.

It also assisted with the test suite, documentation, and debugging.

I do not claim that all implementation code was independently written without AI assistance.

## 5. My review and validation

I personally:
- Executed the project in Google Colab.
- Ran the pipeline and examined its accepted and rejected record counts.
- Investigated duplicate handling and retry behavior.
- Executed the automated test suite, which passed all four tests.
- Ran baseline and salted aggregation and join benchmarks.
- Compared output correctness and partition distributions.
- Reviewed the README, project structure, and performance report.
- Published the project to my public GitHub repository.

## 6. Important engineering finding

The experiments showed that salting improved partition balance but did not improve execution time in the tested local environment.

I retained this result in the documentation rather than claiming an unsupported performance improvement.

## 7. Limitations of this prompt-history record

This document is reconstructed from the available ChatGPT project conversation records. It summarizes the main development requests and does not reproduce every original prompt verbatim.

The AI responses and intermediate code suggestions are not reproduced in full. The project implementation, testing results, and documentation should be considered alongside this disclosure.
