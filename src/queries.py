"""Database queries for Pro mode."""
import duckdb
import streamlit as st

from config import (
    PRO_PARQUET_DATASET,
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
    PRO_TOP_SPECIES_COUNT,
)


@st.cache_resource
def get_duckdb_connection():
    """Create and configure DuckDB connection."""
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


def check_user_has_annotations(user_id):
    """
    Check if a user has any assigned annotations in the database.
    
    Args:
        user_id: The user ID to check
        
    Returns:
        bool: True if user has annotations, False otherwise
    """
    try:
        conn = get_duckdb_connection()
        query = f"""
        SELECT COUNT(*) as count
        FROM '{PRO_PARQUET_DATASET}'
        WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
        """
        result = conn.execute(query, [user_id]).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False


@st.cache_data
def get_top_species_for_database():
    """
    Get the top N species from the database based on detection count.
    These will be used for the checklist validation.
    
    Returns:
        list: List of scientific names as Python list (not tuples)
    """
    conn = get_duckdb_connection()
    query = f"""
    SELECT "scientific name", COUNT(*) as detection_count
    FROM '{PRO_PARQUET_DATASET}'
    GROUP BY "scientific name"
    ORDER BY detection_count DESC
    LIMIT {PRO_TOP_SPECIES_COUNT}
    """
    result = conn.execute(query).fetchall()
    return [row[0] for row in result]


@st.cache_data(ttl=600, show_spinner="Loading assigned clips...")  # Cache for 10 minutes
def get_assigned_clips_for_user(user_id):
    """
    Get all assigned clips for a specific userID.
    Returns list of dicts for efficient processing.
    CACHED to avoid repeated parquet queries.
    
    Args:
        user_id: The user's ID (can be string or int)
        
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
        "max uncertainty" as uncertainty_array,
        userID
    FROM '{PRO_PARQUET_DATASET}'
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
            "uncertainty_array": row[5],
            "userID": row[6]
        }
        for row in results
    ]
    
    return clips


@st.cache_data(ttl=300)  # Cache per user_id
def get_validated_pro_clips(user_id):
    """
    Get clips that have already been validated by this user.
    Cache is automatically keyed by user_id parameter.
    
    Args:
        user_id: The user's ID
        
    Returns:
        set: Set of (filename, start_time) tuples for validated clips
    """
    import tempfile
    from pathlib import Path
    import boto3
    import pandas as pd
    from botocore.client import Config
    from config import PRO_VALIDATIONS_PREFIX, S3_BUCKET, S3_ENDPOINT, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
    
    try:
        # Configure boto3 for S3 access
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
        )
        
        # List all validation files
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PRO_VALIDATIONS_PREFIX)
        
        if "Contents" not in response:
            return set()
        
        # Read and combine all validation files
        all_validations = []
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w+", suffix=".csv", delete=False
                    ) as temp_file:
                        try:
                            s3_client.download_file(S3_BUCKET, obj["Key"], temp_file.name)
                            df = pd.read_csv(temp_file.name)
                            all_validations.append(df)
                        finally:
                            Path(temp_file.name).unlink()
                except Exception:
                    continue
        
        if not all_validations:
            return set()
        
        # Combine all validation dataframes
        combined_df = pd.concat(all_validations, ignore_index=True)
        
        # Filter for matching userID
        filtered = combined_df[
            combined_df["userID"].astype(str) == str(user_id)
        ]
        
        return set(zip(filtered["filename"], filtered["start_time"]))
        
    except Exception:
        return set()


def _get_validated_clips_with_session(user_id):
    """Get all validated clips including session state."""
    validated_clips = get_validated_pro_clips(user_id)
    
    # Add clips validated in current session
    if hasattr(st.session_state, 'pro_validated_clips_session'):
        validated_clips = validated_clips.union(st.session_state.pro_validated_clips_session)
    
    return validated_clips


def get_random_assigned_clip(user_id):
    """
    Get next unvalidated clip assigned to the user.
    Uses deterministic ordering (filename, start_time) for faster queries.
    
    Args:
        user_id: The user's ID
        
    Returns:
        dict: Clip information or None if no clips available
        dict with 'all_validated': True if all clips are validated
    """
    conn = get_duckdb_connection()
    
    # Get validated clips (from cache + session state)
    validated_clips = _get_validated_clips_with_session(user_id)
    
    # If there are validated clips, build exclusion clause
    if validated_clips:
        # Create list of tuples for SQL IN clause
        validated_tuples = [(filename, start_time) for filename, start_time in validated_clips]
        
        # Build query with exclusion - let DuckDB do the filtering
        placeholders = ",".join(["(?, ?)" for _ in validated_tuples])
        exclusion_clause = f"""AND (fullPath, "start time") NOT IN ({placeholders})"""
        
        # Flatten the tuples for query parameters
        exclusion_params = []
        for filename, start_time in validated_tuples:
            exclusion_params.extend([filename, start_time])
    else:
        exclusion_clause = ""
        exclusion_params = []
    
    # Query for next unvalidated clip - use deterministic ordering for speed
    # ORDER BY filename, start_time is much faster than random()
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
    {exclusion_clause}
    ORDER BY fullPath, "start time"
    LIMIT 1
    """
    
    try:
        result = conn.execute(query, [user_id] + exclusion_params).fetchone()
        
        if result:
            return {
                "filename": result[0],
                "deployment_id": result[1],
                "start_time": result[2],
                "species_array": result[3],
                "confidence_array": result[4],
                "uncertainty_array": result[5],
                "userID": result[6],
                "all_validated": False
            }
        else:
            # No unvalidated clips found - check if user has any clips at all
            count_query = f"""
            SELECT COUNT(*) as count
            FROM '{PRO_PARQUET_DATASET}'
            WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
            """
            total = conn.execute(count_query, [user_id]).fetchone()[0]
            
            if total > 0:
                return {
                    "all_validated": True,
                    "total_clips": total
                }
            else:
                return None
    except Exception as e:
        st.error(f"Error fetching random clip: {e}")
        return None


@st.cache_data
def get_remaining_pro_clips_count(user_id):
    """Get count of remaining unvalidated clips for user.
    Uses DuckDB COUNT for better performance."""
    conn = get_duckdb_connection()
    
    # Get validated clips (from cache + session state)
    validated_clips = _get_validated_clips_with_session(user_id)
    
    # If there are validated clips, build exclusion clause
    if validated_clips:
        validated_tuples = [(filename, start_time) for filename, start_time in validated_clips]
        placeholders = ",".join(["(?, ?)" for _ in validated_tuples])
        exclusion_clause = f"""AND (fullPath, "start time") NOT IN ({placeholders})"""
        
        exclusion_params = []
        for filename, start_time in validated_tuples:
            exclusion_params.extend([filename, start_time])
    else:
        exclusion_clause = ""
        exclusion_params = []
    
    # Use DuckDB COUNT - much faster than Python iteration
    query = f"""
    SELECT COUNT(*) as count
    FROM '{PRO_PARQUET_DATASET}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    {exclusion_clause}
    """
    
    try:
        result = conn.execute(query, [user_id] + exclusion_params).fetchone()
        return result[0] if result else 0
    except Exception:
        return 0
