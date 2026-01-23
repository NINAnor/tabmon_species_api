"""Shared utility functions for Pro mode."""
import tempfile
from pathlib import Path

import boto3
import librosa
import pandas as pd
import streamlit as st
from botocore.client import Config

from config import (
    BIRDNET_MULTILINGUAL_PATH,
    S3_ACCESS_KEY_ID,
    S3_BASE_URL,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
)


@st.cache_data
def load_species_translations():
    """Load the multilingual species name translations"""
    return pd.read_csv(BIRDNET_MULTILINGUAL_PATH)


def get_species_display_names(species_list, language_code):
    """Convert scientific names to display names in selected language
    
    Args:
        species_list: List of scientific names
        language_code: Language code (e.g., 'en_uk', 'es', 'nl') or 'Scientific_Name'
        
    Returns:
        Dictionary mapping display names to scientific names
    """
    if language_code == "Scientific_Name":
        return {species: species for species in species_list}

    translations_df = load_species_translations()
    species_map = {}

    for species in species_list:
        translation_row = translations_df[translations_df["Scientific_Name"] == species]
        if not translation_row.empty and language_code in translation_row.columns:
            translated_name = translation_row[language_code].iloc[0]
            if pd.notna(translated_name):
                species_map[translated_name] = species
            else:
                species_map[species] = species  # Fallback to scientific name
        else:
            species_map[species] = species  # Fallback to scientific name

    return species_map


@st.cache_data(ttl=600, show_spinner=False)  # Cache for 10 minutes, no spinner (parent shows it)
def extract_clip(s3_url, start_time, sr=48000):
    """Extract a 9-second audio clip from S3 file (3s before + 6s after detection)."""

    if s3_url is None:
        st.error("Could not find audio file in S3")
        return None

    # Configure boto3 for S3 access
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    # Parse S3 URL to get bucket and key
    # s3://bucket/path/to/file.wav -> bucket='bucket', key='path/to/file.wav'
    s3_parts = s3_url.replace("s3://", "").split("/", 1)
    bucket = s3_parts[0]
    key = s3_parts[1] if len(s3_parts) > 1 else ""

    # Create temporary file to download audio
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
        try:
            # Download file from S3
            s3_client.download_file(bucket, key, temp_file.name)

            # Load and extract clip using librosa
            audio_data, _ = librosa.load(temp_file.name, sr=sr, mono=True)
            start_sample = int((start_time - 3) * sr)
            end_sample = int((start_time + 6) * sr)
            clip = audio_data[start_sample:end_sample]

            return clip

        finally:
            # Clean up temporary file
            Path(temp_file.name).unlink()


def save_pro_validation_response(validation_data):
    """
    Save Pro mode validation to session-specific file.
    Pro validations include multiple species in a list format.
    """
    from config import PRO_VALIDATIONS_PREFIX, S3_BUCKET
    
    session_id = st.session_state.session_id

    # Configure boto3 for S3 access
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    bucket = S3_BUCKET

    # Use session ID as filename: validations_pro/session_{session_id}.csv
    key = f"{PRO_VALIDATIONS_PREFIX}/session_{session_id}.csv"

    # Convert arrays to pipe-separated strings for clean CSV storage
    validation_data_copy = validation_data.copy()
    
    # Helper to convert list/array to pipe-separated string
    def list_to_string(value):
        if isinstance(value, list):
            return "|".join(str(item) for item in value)
        elif isinstance(value, str):
            # Already a string (might be from parquet), keep as is
            return value
        return value
    
    # Convert all array fields
    validation_data_copy["identified_species"] = list_to_string(validation_data_copy.get("identified_species"))
    validation_data_copy["birdnet_species_detected"] = list_to_string(validation_data_copy.get("birdnet_species_detected"))
    validation_data_copy["birdnet_confidences"] = list_to_string(validation_data_copy.get("birdnet_confidences"))
    validation_data_copy["birdnet_uncertainties"] = list_to_string(validation_data_copy.get("birdnet_uncertainties"))
    
    validation_df = pd.DataFrame([validation_data_copy])

    try:
        # Try to append to existing session file
        try:
            s3_client.head_object(Bucket=bucket, Key=key)

            # File exists, download it, append, and re-upload
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".csv", delete=False
            ) as temp_file:
                try:
                    s3_client.download_file(bucket, key, temp_file.name)
                    existing_df = pd.read_csv(temp_file.name)
                    combined_df = pd.concat(
                        [existing_df, validation_df], ignore_index=True
                    )
                    combined_df.to_csv(temp_file.name, index=False)
                    s3_client.upload_file(temp_file.name, bucket, key)
                finally:
                    Path(temp_file.name).unlink()
        except Exception:
            # File doesn't exist yet, create new one
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".csv", delete=False
            ) as temp_file:
                try:
                    validation_df.to_csv(temp_file.name, index=False)
                    s3_client.upload_file(temp_file.name, bucket, key)
                finally:
                    Path(temp_file.name).unlink()

        # Track validated clip in session state to avoid showing it again
        # without needing to clear caches and re-query
        if 'pro_validated_clips_session' not in st.session_state:
            st.session_state.pro_validated_clips_session = set()
        
        clip_key = (validation_data["filename"], validation_data["start_time"])
        st.session_state.pro_validated_clips_session.add(clip_key)
        
        # Decrement remaining count in session state (faster than re-querying)
        if 'pro_remaining_count' in st.session_state and st.session_state.pro_remaining_count is not None:
            st.session_state.pro_remaining_count = max(0, st.session_state.pro_remaining_count - 1)
        
        # Only clear count cache when needed (on next load it will refresh from DB)
        from queries import get_remaining_pro_clips_count
        get_remaining_pro_clips_count.clear()
        return True
    except Exception as e:
        st.error(f"Error saving Pro validation: {e}")
        return False
