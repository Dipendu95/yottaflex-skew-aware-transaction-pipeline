# Data Dictionary

## 1. Transaction Dataset

**File:** `transactions.csv`

**Grain:** One row represents one transaction.

| Column | Data type | Description |
|---|---|---|
| transaction_id | String | Unique transaction identifier; primary key. |
| merchant_id | String | Merchant identifier; references merchant master data. |
| customer_id | String | Customer identifier associated with the transaction. |
| transaction_amount | Decimal(12,2) | Transaction amount in the dataset's currency. |
| payment_method | String | Payment channel, such as UPI, CREDIT_CARD, DEBIT_CARD, or CASH. |
| transaction_status | String | Transaction outcome: SUCCESS or FAILED. |
| transaction_timestamp | Timestamp | Date and time of the transaction, represented in UTC. |

## 2. Merchant Reference Dataset

**File:** `merchants.csv`

**Grain:** One row represents one merchant.

| Column | Data type | Description |
|---|---|---|
| merchant_id | String | Unique merchant identifier; primary key. |
| merchant_name | String | Merchant display name. |
| merchant_tier | String | Merchant classification, such as PREMIUM or STANDARD. |

## 3. Curated Transaction Dataset

**Format:** Parquet

| Column | Data type | Description |
|---|---|---|
| transaction_id | String | Unique transaction identifier. |
| merchant_id | String | Validated merchant identifier. |
| customer_id | String | Customer identifier. |
| amount | Decimal(12,2) | Validated transaction amount. |
| payment_method | String | Validated payment method. |
| transaction_status | String | Validated transaction status. |
| transaction_timestamp | Timestamp | Parsed transaction timestamp in UTC. |
| run_id | String | Identifier of the pipeline execution. |
| source_file | String | Source file name associated with ingestion. |

## 4. Data Quality and Relationships

- `transaction_id` identifies a transaction and is checked for duplicates.
- `merchant_id` in transactions must exist in the merchant reference dataset.
- Transaction amounts must be positive.
- Payment method and transaction status must belong to the accepted values.
- Invalid records are quarantined with rejection reasons.
- Timestamps are parsed and standardized using UTC.

## 5. Important Implementation Notes

- Raw transaction fields are initially ingested as strings for controlled validation.
- The curated dataset uses an explicit schema and decimal amount precision.
- Structural CSV errors are separated from business validation errors.
- The source dataset is synthetic and intended for pipeline engineering experiments.
