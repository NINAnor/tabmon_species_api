"""
Pro Mode Database Queries

This module contains database queries specific to the Pro mode,
which uses a different database structure with userID and assigned annotations.
"""

import duckdb
import streamlit as st

from shared.config import (
    PRO_PARQUET_DATASET,
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
    PRO_TOP_SPECIES_COUNT,
)


@st.cache_resource
def get_pro_duckdb_connection():
    """Create and configure DuckDB connection for Pro database."""
    conn = duckdb.connect()

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Configure S3 access (uses same config as normal mode)
    conn.execute("SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute("SET s3_use_ssl=true;")
    conn.execute("SET s3_url_style='path';")

    return conn


def check_user_has_annotations(user_id):
    """
    Check if a user has any assigned annotations in the database.
    
    Args:
        user_id: The user ID to check
        
    Returns:
        bool: True if user has annotations, False otherwise
    """
    try:
        conn = get_pro_duckdb_connection()
        query = f"""
        SELECT COUNT(*) as count
        FROM '{PRO_PARQUET_DATASET}'
        WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
        """
        result = conn.execute(query, [user_id]).fetchone()
        return result[0] > 0 if result else False
    except Exception as e:
        # If there's an error querying (e.g., file doesn't exist), return False
        return False


@st.cache_data
def get_available_user_ids():
    """Get list of userIDs that have assigned annotations."""
    conn = get_pro_duckdb_connection()
    query = f"""
    SELECT DISTINCT userID
    FROM '{PRO_PARQUET_DATASET}'
    WHERE userID IS NOT NULL
    ORDER BY userID
    """
    result = conn.execute(query).fetchall()
    return [row[0] for row in result]


@st.cache_data
def get_top_species_for_database():
    """
    Get the top N species from the Pro database based on detection count.
    These will be used for the checklist validation.
    
    Returns:
        list: List of scientific names as Python list (not tuples)
    """
    conn = get_pro_duckdb_connection()
    query = f"""
    SELECT "scientific name", COUNT(*) as detection_count
    FROM '{PRO_PARQUET_DATASET}'
    GROUP BY "scientific name"
    ORDER BY detection_count DESC
    LIMIT {PRO_TOP_SPECIES_COUNT}
    """
    result = conn.execute(query).fetchall()
    # Return as Python list of strings
    return [row[0] for row in result]


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_assigned_clips_for_user(user_id, confidence_threshold=0.0):
    """
    Get all assigned clips for a specific userID.
    Returns list of dicts for efficient processing.
    
    Args:
        user_id: The user's ID (can be string or int)
        confidence_threshold: Minimum confidence score filter
        
    Returns:
        List of dicts with clip information
    """
    conn = get_pro_duckdb_connection()
    query = f"""
    SELECT 
        fullPath as filename,
        deployment_id,
        "start time" as start_time,
        "scientific name" as species_array,
        confidence as confidence_array,
        "max uncertainty" as uncertainty_array,
        userID
    FROM '{PRO_PARQUET_DATASET}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    ORDER BY filename, start_time
    """
    results = conn.execute(query, [user_id]).fetchall()
    
    # Return as list of dicts for efficient processing
    clips = [
        {
            "filename": row[0],
            "deployment_id": row[1],
            "start_time": row[2],
            "species_array": row[3],
            "confidence_array": row[4],
            "uncertainty_array": row[5],
            "userID": row[6]
        }
        for row in results
    ]
    
    return clips


@st.cache_data
def get_validated_pro_clips(user_id):
    """
    Get clips that have already been validated by this user.
    
    Args:
        user_id: The user's ID
        
    Returns:
        set: Set of (filename, start_time) tuples for validated clips
    """
    from shared.utils import load_pro_validation_responses
    
    try:
        validated_df = load_pro_validation_responses()
        if validated_df is not None and not validated_df.empty:
            user_validated = validated_df[validated_df["userID"] == user_id]
            return set(
                zip(
                    user_validated["filename"],
                    user_validated["start_time"]
                )
            )
    except Exception:
        pass
    
    return set()


def get_random_assigned_clip(user_id, confidence_threshold=0.0):
    """
    Get a random unvalidated clip assigned to the user.
    
    Args:
        user_id: The user's ID
        confidence_threshold: Minimum confidence score
        
    Returns:
        dict: Clip information or None if no clips available
        dict with 'all_validated': True if all clips are validated
    """
    all_clips = get_assigned_clips_for_user(user_id, confidence_threshold)
    
    if not all_clips:
        return None
    
    # Get validated clips as a set for fast lookup
    validated_clips = get_validated_pro_clips(user_id)
    
    # Filter out validated clips using list comprehension (much faster than .apply())
    unvalidated = [
        clip for clip in all_clips
        if (clip["filename"], clip["start_time"]) not in validated_clips
    ]
    
    # If all clips are validated
    if not unvalidated:
        return {
            "all_validated": True,
            "total_clips": len(all_clips)
        }
    
    # Get random clip from unvalidated
    import random
    random_clip = random.choice(unvalidated)
    
    return {
        "filename": random_clip["filename"],
        "species_array": random_clip["species_array"],
        "confidence_array": random_clip["confidence_array"],
        "uncertainty_array": random_clip["uncertainty_array"],
        "start_time": random_clip["start_time"],
        "deployment_id": random_clip["deployment_id"],
        "userID": random_clip["userID"],
        "all_validated": False
    }


@st.cache_data
def get_remaining_pro_clips_count(user_id, confidence_threshold=0.0):
    """Get count of remaining unvalidated clips for user."""
    all_clips = get_assigned_clips_for_user(user_id, confidence_threshold)
    validated_clips = get_validated_pro_clips(user_id)
    
    # Count unvalidated clips efficiently using sum with generator expression
    unvalidated_count = sum(
        1 for clip in all_clips
        if (clip["filename"], clip["start_time"]) not in validated_clips
    )
    
    return unvalidated_count
