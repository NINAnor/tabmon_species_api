import streamlit as st
from pathlib import Path
import duckdb
import pandas as pd

# DATA PATH FROM THE MOUNT
DATA_PATH = Path("/data")
PARQUET_DATASET = "/data/merged_predictions_light/*/*/*.parquet"
SITE_INFO_PATH = DATA_PATH / "site_info.csv"

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

def match_device_id_to_site(site_info_path):
    site_info_df = pd.read_csv(site_info_path)
    
    device_site_map = {}
    for _, row in site_info_df.iterrows():
        device_site_map[row["DeviceID"]] = row["Site"]

    return device_site_map

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


def get_random_detection_clip(country, device_id, species):
    conn = get_duckdb_connection()
    query = f"""
    SELECT filename, "start time", confidence
    FROM '{PARQUET_DATASET}'
    WHERE country = ? AND device_id = ? AND "scientific name" = ?
    ORDER BY RANDOM()
    LIMIT 1
    """
    result = conn.execute(query, [country, device_id, species]).fetchone()
    if result:
        return {
            "filename": result[0],
            "start_time": result[1],
            "confidence": result[2],
        }
    return None