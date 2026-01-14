import os
from pathlib import Path

# S3 Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

# S3 paths (replace /data with s3:// URLs)
S3_BASE_URL = f"s3://{S3_BUCKET}"
PARQUET_DATASET = f"{S3_BASE_URL}/merged_predictions_light/*/*/*.parquet"
SITE_INFO_S3_PATH = f"{S3_BASE_URL}/site_info.csv"

# Local paths (for files that need to be local)
VALIDATION_RESPONSES_S3_PATH = f"{S3_BASE_URL}/validation_responses.csv"
BIRDNET_MULTILINGUAL_PATH = (
    Path(__file__).parent.parent / "assets" / "birdnet_multilingual.csv"
)

# Language configuration
LANGUAGE_MAPPING = {
    "English": "en_uk",
    "Spanish": "es",
    "Dutch": "nl",
    "Norwegian": "no",
    "French": "fr",
}
