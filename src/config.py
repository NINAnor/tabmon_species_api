"""Configuration shared by the citizen and expert apps."""

import os
from pathlib import Path

ASSETS_DIR = Path(__file__).parent.parent / "assets"

# ============================================================================
# S3 Configuration
# ============================================================================

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BASE_URL = f"s3://{S3_BUCKET}"

# Site info CSV for mapping device IDs to site names
SITE_INFO_S3_PATH = f"{S3_BASE_URL}/site_info.csv"

# ============================================================================
# Audio Configuration
# ============================================================================

SAMPLE_RATE = 48000

# Length of a BirdNET detection window, in seconds
DETECTION_SECONDS = 3

# Fixed gain applied to all clips uniformly. No peak normalization, so relative
# loudness differences between clips are preserved.
try:
    AUDIO_GAIN = float(os.getenv("AUDIO_GAIN", "0.9"))
except ValueError:
    AUDIO_GAIN = 0.9

# ============================================================================
# Citizen App Configuration
# ============================================================================

# BirdNET predictions, partitioned as country=<country>/device_id=<id>/*.parquet
PREDICTIONS_S3_PREFIX = os.getenv(
    "PREDICTIONS_S3_PREFIX", "tabmon_predictions/merged_predictions_light"
).strip("/")

# Raw audio is stored under <prefix><country suffix>/<device>/conf_*/<filename>
AUDIO_S3_PREFIX = "proj_tabmon_NINA"
AUDIO_COUNTRY_SUFFIXES = {
    "France": "_FR",
    "Spain": "_ES",
    "Netherlands": "_NL",
    "Norway": "",
}

# Default seconds of audio played before and after the detection window
CITIZEN_CONTEXT_SECONDS = 3

# Citizen validations stored in: validations/session_{session_id}.csv
CITIZEN_VALIDATIONS_PREFIX = "validations"

# ============================================================================
# Expert App Configuration
# ============================================================================

# Expert mode datasets folder
EXPERT_DATASETS_PREFIX = "validation_dataset"

# Expert validations stored in: validations_expert/{dataset}_session_{session_id}.csv
EXPERT_VALIDATIONS_PREFIX = "validations_expert"

# ============================================================================
# Language Configuration
# ============================================================================

# Language mapping for species names
LANGUAGE_MAPPING = {
    "English": "en_uk",
    "Spanish": "es",
    "Dutch": "nl",
    "Norwegian": "no",
    "French": "fr",
}

# Path to multilingual species names CSV
BIRDNET_MULTILINGUAL_PATH = ASSETS_DIR / "birdnet_multilingual.csv"

LOGO_PATH = ASSETS_DIR / "tabmon_logo.png"
