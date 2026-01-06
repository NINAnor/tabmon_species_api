import streamlit as st
from pathlib import Path
import duckdb
import pandas as pd

from utils import get_validated_clips
from config import PARQUET_DATASET, SITE_INFO_PATH


# Initialize DuckDB connection
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

def get_random_detection_clip(country, device_id, species, confidence_threshold=0.0):
    conn = get_duckdb_connection()
    
    # Get already validated clips
    validated_clips = get_validated_clips(country, device_id, species)
    
    # Get all potential clips with confidence filter
    query = f"""
    SELECT filename, "start time", confidence
    FROM '{PARQUET_DATASET}'
    WHERE country = ? AND device_id = ? AND "scientific name" = ? AND confidence >= ?
    ORDER BY RANDOM()
    """
    results = conn.execute(query, [country, device_id, species, confidence_threshold]).fetchall()
    
    # Find first clip that hasn't been validated
    for result in results:
        clip_key = (result[0], result[1])  # (filename, start_time)
        if clip_key not in validated_clips:
            return {
                "filename": result[0],
                "start_time": result[1],
                "confidence": result[2],
            }
    
    if results and len(validated_clips) > 0:
        return {
            "all_validated": True,
            "total_clips": len(results),
            "validated_count": len(validated_clips)
        }
    return None