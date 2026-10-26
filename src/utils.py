"""Shared utility functions for Expert mode."""

import ast
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
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
)


def parse_list_cell(value):
    """Parse a CSV cell that may contain a list representation.

    Cells written by `save_pro_validation_response` hold Python-list
    reprs (e.g. "['Other animal', 'Bird']"). Older rows contain a single
    plain value or 'nan'. Always returns a list of strings.
    """
    text = "" if value is None else str(value)
    text = "" if text.lower() == "nan" else text.strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list | tuple):
                return [str(item) for item in parsed]
        except (ValueError, SyntaxError):
            # Fall through: keep the raw cell text as a single item.
            return [text]
    return [text]


@st.cache_data
def load_species_translations():
    """Load the multilingual species name translations"""
    return pd.read_csv(BIRDNET_MULTILINGUAL_PATH)


def get_species_display_names(species_list, language_code):
    """Convert scientific names to display names in selected language."""
    if language_code == "Scientific_Name":
        return {species: species for species in species_list}

    translations_df = load_species_translations()
    species_map = {}

    for species in species_list:
        translation_row = translations_df[translations_df["Scientific_Name"] == species]
        translated_name = species  # Default fallback

        if not translation_row.empty and language_code in translation_row.columns:
            trans = translation_row[language_code].iloc[0]
            if pd.notna(trans):
                translated_name = trans

        species_map[translated_name] = species

    return species_map


@st.cache_data(ttl=600, show_spinner=False)
def extract_clip(s3_url, start_time, sr=48000, context_seconds=1):
    """Extract audio clip from S3 with configurable context padding.

    Extracts `context_seconds` before and after the 3-second BirdNET detection
    window (total duration = 3 + 2 * context_seconds).
    """
    if not s3_url:
        st.error("Could not find audio file in S3")
        return None

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    bucket, key = s3_url.replace("s3://", "").split("/", 1)

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
        try:
            s3_client.download_file(bucket, key, temp_file.name)
            audio_data, _ = librosa.load(temp_file.name, sr=sr, mono=True)
            start_sample = int(max(0, (start_time - context_seconds) * sr))
            end_sample = int(min(len(audio_data), (start_time + 3 + context_seconds) * sr))
            return audio_data[start_sample:end_sample]
        finally:
            Path(temp_file.name).unlink()


def save_pro_validation_response(validation_data):
    """
    Save Expert mode validation to session-specific file.
    Expert validations include multiple species in a list format.
    """
    from config import EXPERT_VALIDATIONS_PREFIX, S3_BUCKET

    session_id = st.session_state.session_id

    # Get dataset name from session state
    dataset_path = st.session_state.get("expert_selected_dataset", "")
    dataset_name = (
        dataset_path.split("/")[-1].replace(".parquet", "")
        if dataset_path
        else "unknown"
    )

    # Configure boto3 for S3 access
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    bucket = S3_BUCKET

    key = f"{EXPERT_VALIDATIONS_PREFIX}/{dataset_name}_session_{session_id}.csv"

    # Convert arrays to pipe-separated strings for clean CSV storage
    def list_to_string(value):
        return (
            "|".join(str(item) for item in value) if isinstance(value, list) else value
        )

    def _list_repr_cell(value):
        """Serialize a multi-value column as a list cell (user preference):
        e.g. "['Other animal', 'Bird']". pandas' to_csv quotes the cell
        because of the commas, and parse_list_cell reads it back."""
        if isinstance(value, list):
            return str(value)
        return value

    validation_data_copy = {
        **validation_data,
        "identified_species": list_to_string(validation_data.get("identified_species")),
        "birdnet_species_detected": list_to_string(
            validation_data.get("birdnet_species_detected")
        ),
        "birdnet_confidences": list_to_string(
            validation_data.get("birdnet_confidences")
        ),
        "vocalization_types": "|".join(
            f"{species}:{vtype}"
            for species, vtype in (
                validation_data.get("vocalization_types") or {}
            ).items()
        ),
        "user_notes": list_to_string(validation_data.get("user_notes")),
        "is_cross_validation": validation_data.get("is_cross_validation", False),
        "is_update": validation_data.get("is_update", False),
        "is_fp_assessment": validation_data.get("is_fp_assessment", False),
        "fp_target_species": list_to_string(
            validation_data.get("fp_target_species")
        ),
        "fp_target_species_confidence": list_to_string(
            validation_data.get("fp_target_species_confidence")
        ),
        "fp_likely_mechanism": list_to_string(
            validation_data.get("fp_likely_mechanism")
        ),
        "fp_source_category": _list_repr_cell(
            validation_data.get("fp_source_category")
        ),
        "fp_source_detail": _list_repr_cell(
            validation_data.get("fp_source_detail")
        ),
    }

    validation_df = pd.DataFrame([validation_data_copy])

    def save_to_s3(df, bucket, key):
        """Helper to save DataFrame to S3."""
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False
        ) as temp_file:
            try:
                df.to_csv(temp_file.name, index=False)
                s3_client.upload_file(temp_file.name, bucket, key)
            finally:
                Path(temp_file.name).unlink()

    try:
        # Append to existing file or create new one
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".csv", delete=False
            ) as temp_file:
                try:
                    s3_client.download_file(bucket, key, temp_file.name)
                    existing_df = pd.read_csv(temp_file.name)
                    combined_df = pd.concat(
                        [existing_df, validation_df], ignore_index=True
                    )
                    save_to_s3(combined_df, bucket, key)
                finally:
                    Path(temp_file.name).unlink()
        except Exception:
            save_to_s3(validation_df, bucket, key)

        # Bust the cached validation-records lookup so the just-saved row
        # (and updated remaining-count) is picked up on the next render.
        from database.queries import get_user_validation_records

        get_user_validation_records.clear()
        return True
    except Exception as e:
        st.error(f"Error saving Expert validation: {e}")
        return False
