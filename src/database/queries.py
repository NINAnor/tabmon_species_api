"""Database queries for Expert mode."""

import duckdb
import streamlit as st

from config import (
    EXPERT_DATASETS_FOLDER,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
    SITE_INFO_S3_PATH,
)


@st.cache_resource
def get_duckdb_connection():
    """Create and configure DuckDB connection."""
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute("SET s3_use_ssl=true;")
    conn.execute("SET s3_url_style='path';")
    return conn


@st.cache_data(ttl=3600)
def get_device_site_map():
    """Load device_id to site/cluster/country mapping from site_info.csv on S3.

    Returns dict mapping device_id -> {"site": ..., "cluster": ..., "country": ...}.
    Also supports lookup by deployment_id.
    """
    try:
        conn = get_duckdb_connection()
        df = conn.execute(
            f"SELECT DeviceID, DeploymentID, Site, Cluster, Country"
            f" FROM '{SITE_INFO_S3_PATH}'"
        ).df()
        result = {}
        for _, row in df.iterrows():
            info = {
                "site": row["Site"],
                "cluster": row["Cluster"],
                "country": row["Country"],
            }
            result[row["DeviceID"]] = info
            if row["DeploymentID"]:
                result[row["DeploymentID"]] = info
        return result
    except Exception:
        return {}


def get_site_info_for_deployment(deployment_id):
    """Look up site/cluster/country metadata for a clip's deployment_id.

    Tries a direct deployment_id lookup first, then falls back to the
    device_id (last segment after '_').

    Returns the info dict, or {} when nothing matches.
    """
    device_site_map = get_device_site_map()
    site_info = device_site_map.get(deployment_id)
    if site_info:
        return site_info
    device_id = (
        deployment_id.rsplit("_", 1)[-1] if "_" in deployment_id else deployment_id
    )
    return device_site_map.get(device_id, {})


def list_available_datasets():
    """List all available parquet datasets in the validation_dataset folder."""
    import boto3
    from botocore.client import Config

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
        )

        # Remove s3:// prefix and get folder path
        folder_path = EXPERT_DATASETS_FOLDER.replace(f"s3://{S3_BUCKET}/", "")

        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=folder_path,
        )

        if "Contents" not in response:
            return []

        datasets = []
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".parquet"):
                filename = key.split("/")[-1]
                datasets.append(
                    {
                        "name": filename.replace(".parquet", ""),
                        "path": f"s3://{S3_BUCKET}/{key}",
                        "size": obj["Size"],
                    }
                )

        return sorted(datasets, key=lambda x: x["name"])
    except Exception as e:
        st.error(f"Error listing datasets: {e}")
        return []


def check_user_has_annotations(user_id, dataset_path=None):
    """Check if user has any assigned annotations in the selected dataset."""
    if dataset_path is None:
        dataset_path = st.session_state.get("expert_selected_dataset")
        if not dataset_path:
            return False

    try:
        conn = get_duckdb_connection()
        query = (
            f"SELECT COUNT(*) FROM '{dataset_path}' "
            "WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)"
        )
        result = conn.execute(query, [user_id]).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False


@st.cache_data(ttl=300)
def get_species_for_user(user_id, dataset_path):
    """Get unique species available for a user in the dataset.

    Returns list of species names sorted alphabetically.
    """
    conn = get_duckdb_connection()
    query = f"""
    SELECT DISTINCT unnest("scientific name") as species
    FROM '{dataset_path}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    ORDER BY species
    """
    try:
        results = conn.execute(query, [user_id]).fetchall()
        return [row[0] for row in results]
    except Exception:
        return []


@st.cache_data(
    ttl=1800, show_spinner="Loading assigned clips..."
)  # Cache for 10 minutes
def get_assigned_clips_for_user(user_id, dataset_path):
    """
    Get all assigned clips for a specific userID from the selected dataset.
    Returns list of dicts for efficient processing.
    CACHED to avoid repeated parquet queries.

    Args:
        user_id: The user's ID (can be string or int)
        dataset_path: S3 path to the parquet dataset

    Returns:
        List of dicts with clip information
    """
    conn = get_duckdb_connection()
    query = f"""
    SELECT
        fullPath as filename,
        deployment_id,
        "start time" as start_time,
        "scientific name" as species_array,
        confidence as confidence_array,
        userID
    FROM '{dataset_path}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    ORDER BY filename, start_time
    """
    results = conn.execute(query, [user_id]).fetchall()

    clips = [
        {
            "filename": row[0],
            "deployment_id": row[1],
            "start_time": row[2],
            "species_array": row[3],
            "confidence_array": row[4],
            "userID": row[5],
        }
        for row in results
    ]

    return clips


def filter_clips_by_species(clips, species_filter):
    """Filter a list of clip dicts to those matching any of species_filter.

    Pure Python, operates on the already-loaded full clip list (from
    get_assigned_clips_for_user) rather than re-querying S3.
    """
    if not species_filter:
        return clips
    species_set = set(species_filter)
    return [
        clip
        for clip in clips
        if set(clip.get("species_array") or []) & species_set
    ]


@st.cache_data(ttl=300)
def get_user_validation_records(user_id, dataset_path):
    """Load this user's own submitted validation rows for the dataset.

    Returns dict {(filename, start_time): row_dict} keeping only the most
    recent row (by timestamp) per clip, so prior corrections (is_update=True
    rows) are reflected rather than the original mistaken submission.
    """
    import tempfile
    from pathlib import Path

    import boto3
    import pandas as pd
    from botocore.client import Config

    from config import (
        EXPERT_VALIDATIONS_PREFIX,
        S3_ACCESS_KEY_ID,
        S3_BUCKET,
        S3_ENDPOINT,
        S3_SECRET_ACCESS_KEY,
    )

    dataset_name = (
        dataset_path.split("/")[-1].replace(".parquet", "") if dataset_path else ""
    )

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
        )

        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=EXPERT_VALIDATIONS_PREFIX
        )
        if "Contents" not in response:
            return {}

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
                except Exception as e:
                    st.warning(f"Skipping invalid file {obj['Key']}: {e}")
                    continue

        if not all_validations:
            return {}

        combined_df = pd.concat(all_validations, ignore_index=True)
        filtered = combined_df[combined_df["userID"].astype(str) == str(user_id)]
        if filtered.empty:
            return {}

        # Keep only the latest row (by timestamp) per (filename, start_time)
        filtered = filtered.copy()
        filtered["timestamp"] = pd.to_datetime(
            filtered["timestamp"], errors="coerce"
        )
        filtered = filtered.sort_values("timestamp")

        records = {}
        for _, row in filtered.iterrows():
            key = (row["filename"], row["start_time"])
            records[key] = row.to_dict()

        return records

    except Exception:
        return {}


def get_remaining_pro_clips_count(clips, validated_keys):
    """Count how many clips in the (already-filtered) list are unvalidated.

    Pure Python — operates on the full clip list and the set of validated
    keys already loaded in memory, no extra query needed.
    """
    return sum(
        1
        for clip in clips
        if (clip["filename"], clip["start_time"]) not in validated_keys
    )


def get_first_unannotated_index(clips, validated_keys):
    """Return the index of the first not-yet-validated clip, or len(clips)
    if every clip has already been validated."""
    for idx, clip in enumerate(clips):
        if (clip["filename"], clip["start_time"]) not in validated_keys:
            return idx
    return len(clips)
