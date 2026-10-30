
from pathlib import Path

# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Data directories
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"

REPORT_DIR = PROJECT_ROOT / "reports"
LOG_DIR = PROJECT_ROOT / "logs"
DOCS_DIR = PROJECT_ROOT / "docs"

# Reproducibility
RANDOM_SEED = 42

# Dataset configuration
NUM_TRANSACTIONS = 100_000
NUM_MERCHANTS = 1_000
NUM_CUSTOMERS = 20_000

# Data skew configuration
NUM_HOT_MERCHANTS = 5
HOT_MERCHANT_SHARE = 0.60

# Spark configuration
SHUFFLE_PARTITIONS = 8

# Output filenames
TRANSACTION_FILE = RAW_DIR / "transactions.csv"
MERCHANT_FILE = RAW_DIR / "merchants.csv"


