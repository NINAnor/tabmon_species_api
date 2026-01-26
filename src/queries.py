import duckdb
import streamlit as st

from config import (
    S3_ACCESS_KEY_ID,
    S3_BASE_URL,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
    SITE_INFO_S3_PATH,
)
from utils import get_validated_clips


@st.cache_resource
def get_duckdb_connection():
    conn = duckdb.connect()

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Use SET statements instead of CREATE SECRET for better custom endpoint support
    conn.execute("SET s3_region='us-east-1';")  # Use a standard region
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute(
        "SET s3_use_ssl=true;"
    )  # tells DuckDB to use HTTPS instead of HTTP when making S3 requests
    conn.execute(
        "SET s3_url_style='path';"
    )  # controls the URL format DuckDB uses for S3 requests

    return conn


@st.cache_data(ttl=3600)
def load_site_info():
    conn = get_duckdb_connection()
    return conn.execute(f"SELECT * FROM '{SITE_INFO_S3_PATH}'").df()


@st.cache_data(ttl=3600)
def get_available_countries():
    conn = get_duckdb_connection()
    query = f"""
    SELECT DISTINCT Country
    FROM '{SITE_INFO_S3_PATH}'
    WHERE Country IS NOT NULL
    ORDER BY Country
    """
    result = conn.execute(query).fetchall()
    return [row[0] for row in result]


@st.cache_data(ttl=3600, show_spinner="Loading sites...")
def get_sites_for_country(country):
    conn = get_duckdb_connection()
    query = f"""
    SELECT DISTINCT DeviceID
    FROM '{SITE_INFO_S3_PATH}'
    WHERE Country = ?
    ORDER BY DeviceID
    """
    result = conn.execute(query, [country]).fetchall()
    return [row[0] for row in result]


@st.cache_data(ttl=600, show_spinner="Loading species...")
def get_species_for_site(country, device_id):
    conn = get_duckdb_connection()
    # Use Hive partitioning path structure
    targeted_pattern = (
        f"{S3_BASE_URL}/merged_predictions_light/"
        f"country={country}/device_id={device_id}/*.parquet"
    )
    query = f"""
    SELECT "scientific name", COUNT(*) as cnt
    FROM '{targeted_pattern}'
    GROUP BY "scientific name"
    HAVING cnt >= 5
    ORDER BY "scientific name"
    """
    try:
        result = conn.execute(query).fetchall()
        return [row[0] for row in result]
    except Exception:
        return []


def get_audio_files_for_species(country, device_id, species):
    conn = get_duckdb_connection()
    # Use Hive partitioning path structure
    targeted_pattern = (
        f"{S3_BASE_URL}/merged_predictions_light/"
        f"country={country}/device_id={device_id}/*.parquet"
    )
    query = f"""
    SELECT filename
    FROM '{targeted_pattern}'
    WHERE "scientific name" = ?
    LIMIT 10
    """
    try:
        result = conn.execute(query, [species]).fetchall()
        return [row[0] for row in result]
    except Exception:
        return []


@st.cache_data(ttl=3600, show_spinner="Extracting the species at the selected site")
def get_all_clips_for_species(country, device_id, species, confidence_threshold=0.0):
    """Get all clips for a species/location combination with confidence filtering.
    Returns tuple of (all_clips_data, total_count) for efficient processing.
    """
    conn = get_duckdb_connection()
    # Use Hive partitioning path structure
    targeted_pattern = (
        f"{S3_BASE_URL}/merged_predictions_light/"
        f"country={country}/device_id={device_id}/*.parquet"
    )
    query = f"""
    SELECT filename, "start time", confidence
    FROM '{targeted_pattern}'
    WHERE "scientific name" = ? AND confidence >= ?
    ORDER BY confidence DESC
    """
    try:
        results = conn.execute(query, [species, confidence_threshold]).fetchall()

        clips_data = [
            {
                "filename": result[0],
                "start_time": result[1],
                "confidence": result[2],
            }
            for result in results
        ]

        return clips_data, len(results)
    except Exception:
        return [], 0


def get_random_detection_clip(country, device_id, species, confidence_threshold=0.0):
    """Optimized version using cached data for better performance."""
    # Get all clips data (cached)
    all_clips, total_clips = get_all_clips_for_species(
        country, device_id, species, confidence_threshold
    )

    if not all_clips:
        return None

    # Get already validated clips
    validated_clips = get_validated_clips(country, device_id, species)

    # Filter out validated clips
    unvalidated_clips = [
        clip
        for clip in all_clips
        if (clip["filename"], clip["start_time"]) not in validated_clips
    ]

    if not unvalidated_clips:
        return {
            "all_validated": True,
            "total_clips": total_clips,
            "validated_count": len(validated_clips),
        }

    import random

    return random.choice(unvalidated_clips)  # noqa: S311


def get_remaining_clips_count(country, device_id, species, confidence_threshold):
    """Use cached clip data instead of separate database query."""
    # Use cached clip data
    _, total_clips = get_all_clips_for_species(
        country, device_id, species, confidence_threshold
    )

    # Get validated clips count
    validated_clips = get_validated_clips(country, device_id, species)
    validated_count = len(validated_clips)

    return max(0, total_clips - validated_count)
