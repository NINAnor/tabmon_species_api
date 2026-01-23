import os

# ============================================================================
# S3 Configuration
# ============================================================================

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BASE_URL = f"s3://{S3_BUCKET}"

# ============================================================================
# Pro Mode Configuration
# ============================================================================

# Pro mode dataset (with userID field)
PRO_PARQUET_DATASET = f"{S3_BASE_URL}/test_pro_annotations.parquet"

# Pro mode validations stored in: validations_pro/session_{session_id}.csv
PRO_VALIDATIONS_PREFIX = "validations_pro"

# Pro Mode Settings
PRO_TOP_SPECIES_COUNT = int(os.getenv("PRO_TOP_SPECIES_COUNT", "10"))
