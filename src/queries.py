import duckdb
import streamlit as st

from config import PARQUET_DATASET, SITE_INFO_PATH
from utils import get_validated_clips


@st.cache_resource
def get_duckdb_connection():
    return duckdb.connect()


@st.cache_data
def load_site_info():
    conn = get_duckdb_connection()
    return conn.execute(f"SELECT * FROM '{SITE_INFO_PATH}'").df()


@st.cache_data
def get_available_countries():
    conn = get_duckdb_connection()
    query = f"""
    SELECT DISTINCT country
    FROM '{PARQUET_DATASET}'
    ORDER BY country
    """
    result = conn.execute(query).fetchall()
    return [row[0] for row in result]


@st.cache_data
def get_sites_for_country(country):
    conn = get_duckdb_connection()
    query = f"""
    SELECT DISTINCT device_id
    FROM '{PARQUET_DATASET}'
    WHERE country = ?
    ORDER BY device_id
    """
    result = conn.execute(query, [country]).fetchall()
    return [row[0] for row in result]


@st.cache_data
def get_species_for_site(country, device_id):
    conn = get_duckdb_connection()
    query = f"""
    SELECT "scientific name"
    FROM '{PARQUET_DATASET}'
    WHERE country = ? AND device_id = ?
    GROUP BY "scientific name"
    HAVING COUNT(*) >= 5
    ORDER BY "scientific name"
    """
    result = conn.execute(query, [country, device_id]).fetchall()
    return [row[0] for row in result]


def get_audio_files_for_species(country, device_id, species):
    conn = get_duckdb_connection()
    query = f"""
    SELECT filename
    FROM '{PARQUET_DATASET}'
    WHERE country = ? AND device_id = ? AND "scientific name" = ?
    LIMIT 10
    """
    result = conn.execute(query, [country, device_id, species]).fetchall()
    return [row[0] for row in result]


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_clips_for_species(country, device_id, species, confidence_threshold=0.0):
    """Get all clips for a species/location combination with confidence filtering.
    Returns tuple of (all_clips_data, total_count) for efficient processing.
    """
    conn = get_duckdb_connection()
    query = f"""
    SELECT filename, "start time", confidence
    FROM '{PARQUET_DATASET}'
    WHERE country = ? AND device_id = ? AND "scientific name" = ? AND confidence >= ?
    ORDER BY confidence DESC  -- Order by confidence instead of random for consistency
    """
    results = conn.execute(
        query, [country, device_id, species, confidence_threshold]
    ).fetchall()
    
    clips_data = [{
        "filename": result[0],
        "start_time": result[1], 
        "confidence": result[2]
    } for result in results]
    
    return clips_data, len(results)


def get_random_detection_clip(country, device_id, species, confidence_threshold=0.0):
    """Optimized version that uses cached data and session state for better performance."""
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
        clip for clip in all_clips
        if (clip["filename"], clip["start_time"]) not in validated_clips
    ]
    
    if not unvalidated_clips:
        return {
            "all_validated": True,
            "total_clips": total_clips,
            "validated_count": len(validated_clips),
        }
    
    # Return a random unvalidated clip
    import random
    return random.choice(unvalidated_clips)


def get_remaining_clips_count(country, device_id, species, confidence_threshold):
    """Optimized version that uses cached clip data instead of separate database query."""
    # Use cached clip data
    _, total_clips = get_all_clips_for_species(
        country, device_id, species, confidence_threshold
    )
    
    # Get validated clips count
    validated_clips = get_validated_clips(country, device_id, species)
    validated_count = len(validated_clips)
    
    return max(0, total_clips - validated_count)
