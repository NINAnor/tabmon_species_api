from pathlib import Path

# Base data directory
DATA_PATH = Path("/data")

# Dataset paths
PARQUET_DATASET = "/data/merged_predictions_light/*/*/*.parquet"

# CSV file paths
SITE_INFO_PATH = DATA_PATH / "site_info.csv"
VALIDATION_RESPONSES_PATH = DATA_PATH / "validation_responses.csv"