"""Review mode queries - load and filter previously validated clips."""

import tempfile
from pathlib import Path

import boto3
import pandas as pd
import streamlit as st
from botocore.client import Config

from config import (
    EXPERT_VALIDATIONS_PREFIX,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
)


@st.cache_data(ttl=300, show_spinner="Loading validations...")
def load_dataset_validations(dataset_path):
    """Load all validation records for a dataset from S3.

    Reads all CSV files matching the dataset name from the validations_expert/ folder.
    Parses recording month from filename and computes max BirdNET confidence.

    Returns a DataFrame with all validation records plus derived columns.
    """
    import re

    dataset_name = (
        dataset_path.split("/")[-1].replace(".parquet", "") if dataset_path else ""
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=EXPERT_VALIDATIONS_PREFIX
        )
        if "Contents" not in response:
            return pd.DataFrame()

        all_validations = []
        for obj in response["Contents"]:
            filename = obj["Key"].split("/")[-1]
            if obj["Key"].endswith(".csv") and filename.startswith(f"{dataset_name}_"):
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w+", suffix=".csv", delete=False
                    ) as temp_file:
                        try:
                            s3_client.download_file(
                                S3_BUCKET, obj["Key"], temp_file.name
                            )
                            df = pd.read_csv(temp_file.name)
                            all_validations.append(df)
                        finally:
                            Path(temp_file.name).unlink()
                except Exception:
                    continue

        if not all_validations:
            return pd.DataFrame()

        combined_df = pd.concat(all_validations, ignore_index=True)

        # Parse recording date and month from the filename field
        def extract_date(filepath):
            match = re.search(r"(\d{4}-\d{2}-\d{2})T", str(filepath))
            if match:
                return pd.to_datetime(match.group(1))
            return pd.NaT

        combined_df["recording_date"] = combined_df["filename"].apply(extract_date)
        combined_df["recording_month"] = combined_df["recording_date"].dt.month

        # Compute max BirdNET confidence from pipe-separated string
        def parse_max_confidence(conf_str):
            try:
                if pd.isna(conf_str) or conf_str == "":
                    return 0.0
                values = [float(x) for x in str(conf_str).split("|") if x.strip()]
                return max(values) if values else 0.0
            except (ValueError, TypeError):
                return 0.0

        combined_df["max_birdnet_confidence"] = combined_df[
            "birdnet_confidences"
        ].apply(parse_max_confidence)

        # Ensure user_comments is string
        combined_df["user_comments"] = combined_df["user_comments"].fillna("")

        return combined_df

    except Exception as e:
        st.error(f"Error loading validations: {e}")
        return pd.DataFrame()


def get_filtered_validations(dataset_path, filters):
    """Apply filters to validation records and return matching rows.

    Args:
        dataset_path: S3 path to the parquet dataset
        filters: dict with keys:
            - date_range: tuple (start_date, end_date) or None for all
            - confidence_range: tuple (min, max) for BirdNET confidence
            - user_confidence: list of str (Low/Moderate/High) or None for all
            - has_comments: bool - filter to only records with comments
            - comment_search: str - search text within comments
            - species: list of str (scientific names) or None for all
            - validator: str user ID or None for all
            - vocalization_type: str ("Call", "Song") or None for all

    Returns filtered DataFrame.
    """
    df = load_dataset_validations(dataset_path)

    if df.empty:
        return df

    # Filter by date range
    date_range = filters.get("date_range")
    if date_range:
        start_date, end_date = date_range
        df = df[
            (df["recording_date"] >= pd.to_datetime(start_date))
            & (df["recording_date"] <= pd.to_datetime(end_date))
        ]

    # Filter by BirdNET confidence range
    confidence_range = filters.get("confidence_range")
    if confidence_range:
        min_conf, max_conf = confidence_range
        df = df[
            (df["max_birdnet_confidence"] >= min_conf)
            & (df["max_birdnet_confidence"] <= max_conf)
        ]

    # Filter by expert confidence class
    user_confidence = filters.get("user_confidence")
    if user_confidence:
        df = df[df["user_confidence"].isin(user_confidence)]

    # Filter by comments
    if filters.get("has_comments"):
        df = df[df["user_comments"].str.strip() != ""]

    comment_search = filters.get("comment_search", "").strip()
    if comment_search:
        df = df[
            df["user_comments"].str.contains(comment_search, case=False, na=False)
        ]

    # Filter by species (in birdnet_species_detected or identified_species)
    species_filter = filters.get("species")
    if species_filter:
        def contains_species(row):
            detected = str(row.get("birdnet_species_detected", ""))
            identified = str(row.get("identified_species", ""))
            combined = f"{detected}|{identified}"
            return any(sp in combined for sp in species_filter)

        df = df[df.apply(contains_species, axis=1)]

    # Filter by vocalization type (Call/Song)
    vocalization_type = filters.get("vocalization_type")
    if vocalization_type:
        df = df[
            df["vocalization_types"].str.contains(
                vocalization_type, case=False, na=False
            )
        ]

    # Filter by site name
    sites = filters.get("sites")
    if sites:
        df = df[df["site_name"].astype(str).isin(sites)]

    # Filter by validator
    validator = filters.get("validator")
    if validator:
        df = df[df["userID"].astype(str) == str(validator)]

    return df.reset_index(drop=True)


def get_available_validators(dataset_path):
    """Get distinct validator user IDs for a dataset.

    Returns sorted list of user ID strings.
    """
    df = load_dataset_validations(dataset_path)
    if df.empty:
        return []
    return sorted(df["userID"].astype(str).unique().tolist())
