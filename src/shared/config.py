import os
from pathlib import Path

# ============================================================================
# S3 Configuration (shared by both modes)
# ============================================================================

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BASE_URL = f"s3://{S3_BUCKET}"

# Site information (shared)
SITE_INFO_S3_PATH = f"{S3_BASE_URL}/site_info.csv"

# Local paths
BIRDNET_MULTILINGUAL_PATH = (
    Path(__file__).parent.parent.parent / "assets" / "birdnet_multilingual.csv"
)

# Language configuration (shared)
LANGUAGE_MAPPING = {
    "English": "en_uk",
    "Spanish": "es",
    "Dutch": "nl",
    "Norwegian": "no",
    "French": "fr",
}

# ============================================================================
# Normal Mode Configuration
# ============================================================================

# Normal mode dataset
NORMAL_PARQUET_DATASET = f"{S3_BASE_URL}/merged_predictions_light/*/*/*.parquet"

# Normal mode validations stored in: validations/session_{session_id}.csv
NORMAL_VALIDATIONS_PREFIX = "validations"

# ============================================================================
# Pro Mode Configuration
# ============================================================================

# Pro mode dataset (with userID field)
PRO_PARQUET_DATASET = f"{S3_BASE_URL}/test_pro_annotations.parquet"

# Pro mode validations stored in: validations_pro/session_{session_id}.csv
PRO_VALIDATIONS_PREFIX = "validations_pro"

# Pro Mode Settings
PRO_TOP_SPECIES_COUNT = int(os.getenv("PRO_TOP_SPECIES_COUNT", "10"))
