import tempfile
from pathlib import Path

import boto3
import librosa
import pandas as pd
import streamlit as st
from botocore.client import Config

from shared.config import (
    BIRDNET_MULTILINGUAL_PATH,
    S3_ACCESS_KEY_ID,
    S3_BASE_URL,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
)


def extract_clip(s3_url, start_time, sr=48000):
    """Extract a 3-second audio clip from S3 file."""

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


# TODO: THIS IS WHAT TAKES THE LONGEST TIME - OPTIMIZE THIS
def get_single_file_path(filename, country, deployment_id):
    """Get the S3 URL for audio file by searching S3 efficiently."""
    if country == "France":
        suffix = "_FR"
    elif country == "Spain":
        suffix = "_ES"
    elif country == "Netherlands":
        suffix = "_NL"
    elif country == "Norway":
        suffix = ""

    # Configure boto3 for S3 access
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    # get bucket name from url
    bucket = S3_BASE_URL.replace("s3://", "")

    # search directory structure
    country_prefix = f"proj_tabmon_NINA{suffix}/"

    try:
        # First, get all device directories (bugg_RPiID-*)
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=country_prefix, Delimiter="/"
        )

        if "CommonPrefixes" not in response:
            st.warning(f"No device directories found under {country_prefix}")
            return None

        # For each device directory, look for conf_ subdirectories
        for device_prefix in response["CommonPrefixes"]:
            device_path = device_prefix["Prefix"]

            # Get subdirectories under this device (conf_* directories)
            subdir_response = s3_client.list_objects_v2(
                Bucket=bucket, Prefix=device_path, Delimiter="/"
            )

            if "CommonPrefixes" not in subdir_response:
                continue

            # For each conf_ directory, check if our file exists
            for conf_prefix in subdir_response["CommonPrefixes"]:
                conf_path = conf_prefix["Prefix"]
                potential_file_key = f"{conf_path}{filename}"

                # Check if this specific file exists
                try:
                    s3_client.head_object(Bucket=bucket, Key=potential_file_key)
                    return f"s3://{bucket}/{potential_file_key}"
                except s3_client.exceptions.NoSuchKey:
                    continue  # File doesn't exist in this location
                except Exception:  # noqa: S112
                    continue  # Other error, keep searching

    except Exception as e:
        st.error(f"Error searching for file: {e}")
        return None

    st.error(f"Could not find file {filename} in country {country}")
    return None


def save_validation_response(validation_data):
    """Save validation to session-specific file to avoid concurrent write issues."""
    from shared.config import NORMAL_VALIDATIONS_PREFIX
    
    session_id = st.session_state.session_id

    # Configure boto3 for S3 access
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    bucket = S3_BASE_URL.replace("s3://", "")

    # Use session ID as filename: validations/session_{session_id}.csv
    key = f"{NORMAL_VALIDATIONS_PREFIX}/session_{session_id}.csv"

    validation_df = pd.DataFrame([validation_data])

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

        get_validated_clips.clear()
        return True
    except Exception as e:
        st.error(f"Error saving validation: {e}")
        return False


@st.cache_data(ttl=300)
def get_validated_clips(country, device_id, species):
    """Read all validation files from S3 and return validated clips set."""
    from shared.config import NORMAL_VALIDATIONS_PREFIX
    
    try:
        # Configure boto3 for S3 access
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
        )

        bucket = S3_BASE_URL.replace("s3://", "")

        # List all validation files in the normal validation prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=f"{NORMAL_VALIDATIONS_PREFIX}/"
        )

        if "Contents" not in response:
            return set()  # No validation files yet

        # Read and combine all validation files
        all_validations = []
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w+", suffix=".csv", delete=False
                    ) as temp_file:
                        try:
                            s3_client.download_file(bucket, obj["Key"], temp_file.name)
                            df = pd.read_csv(temp_file.name)
                            all_validations.append(df)
                        finally:
                            Path(temp_file.name).unlink()
                except Exception:  # noqa: S112
                    continue  # Skip corrupted files

        if not all_validations:
            return set()

        # Combine all validation dataframes
        validation_df = pd.concat(all_validations, ignore_index=True)

        # Filter for same country, device, and species
        filtered_df = validation_df[
            (validation_df["country"] == country)
            & (validation_df["device_id"] == device_id)
            & (validation_df["species"] == species)
        ]

        # Return set of filename + start_time combinations that have been validated
        validated_clips = set()
        for _, row in filtered_df.iterrows():
            validated_clips.add((row["filename"], row["start_time"]))
        return validated_clips
    except Exception:
        # Error reading validations - return empty set
        return set()


def match_device_id_to_site(site_info_s3_path):
    # Use DuckDB to read from S3 instead of pandas directly
    from shared.queries import get_duckdb_connection

    conn = get_duckdb_connection()
    site_info_df = conn.execute(f"SELECT * FROM '{site_info_s3_path}'").df()

    device_site_map = {}
    for _, row in site_info_df.iterrows():
        device_site_map[row["DeviceID"]] = row["Site"]

    return device_site_map


@st.cache_data
def load_species_translations():
    """Load the multilingual species name translations"""
    return pd.read_csv(BIRDNET_MULTILINGUAL_PATH)


def get_species_display_names(species_list, language_code):
    """Convert scientific names to display names in selected language"""
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


# ============================================================================
# Pro Mode Utility Functions
# ============================================================================


def save_pro_validation_response(validation_data):
    """
    Save Pro mode validation to session-specific file.
    Pro validations include multiple species in a list format.
    """
    from shared.config import PRO_VALIDATIONS_PREFIX, S3_BUCKET
    
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

    # Convert list of species to JSON string for CSV storage
    validation_data_copy = validation_data.copy()
    if isinstance(validation_data_copy.get("identified_species"), list):
        import json
        validation_data_copy["identified_species"] = json.dumps(
            validation_data_copy["identified_species"]
        )
    
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

        return True
    except Exception as e:
        st.error(f"Error saving Pro validation: {e}")
        return False


@st.cache_data(ttl=300)
def load_pro_validation_responses():
    """
    Load all Pro mode validation responses from S3.
    
    Returns:
        DataFrame with all Pro validations or None if no data
    """
    from shared.config import PRO_VALIDATIONS_PREFIX, S3_BUCKET
    
    try:
        # Configure boto3 for S3 access
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
        )

        bucket = S3_BUCKET

        # List all validation files
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=f"{PRO_VALIDATIONS_PREFIX}/"
        )

        if "Contents" not in response:
            return None

        # Download and combine all validation files
        all_validations = []
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv"):
                with tempfile.NamedTemporaryFile(
                    mode="w+", suffix=".csv", delete=False
                ) as temp_file:
                    try:
                        s3_client.download_file(bucket, key, temp_file.name)
                        df = pd.read_csv(temp_file.name)
                        all_validations.append(df)
                    finally:
                        Path(temp_file.name).unlink()

        if all_validations:
            combined_df = pd.concat(all_validations, ignore_index=True)
            
            # Parse JSON strings back to lists for identified_species
            if "identified_species" in combined_df.columns:
                import json
                combined_df["identified_species"] = combined_df["identified_species"].apply(
                    lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) else []
                )
            
            return combined_df
        
        return None
    except Exception:
        return None
