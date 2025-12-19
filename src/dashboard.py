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

def get_full_file_path(files_df, country, deployment_id):

    if country == "France":
        suffix = "_FR"
    elif country == "Spain":
        suffix = "_ES"
    elif country == "Netherlands":
        suffix = "_NL"
    elif country == "Norway":
        suffix = ""

    deviceID = deployment_id.split("_")[-1]
    proj_path = DATA_PATH / f"proj_tabmon_NINA{suffix}"

    #TODO: Optimize by matching the filepath to the index.parquet file
    # For example, take a random filename from the dataframe, and find its full path
    # in the index.parquet that references all the audio files in the project
    def find_actual_path(filename):
        if proj_path.exists():
            device_dirs = list(proj_path.glob(f"bugg_RPiID-*{deviceID}"))
            if device_dirs:
                device_path = device_dirs[0]
                possible_files = list(device_path.glob(f"*/{filename}"))
                if possible_files:
                    return str(possible_files[0])
        return "File not found"

    files_df["full_file_path"] = files_df["Audio files"].apply(find_actual_path)

    return files_df

def main():
    """Main dashboard application."""
    
    # Page configuration
    st.set_page_config(
        page_title="TABMON Species Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🎙️",
    )
    
    st.title("🎙️ TABMON Species Detection Dashboard")
    st.markdown("Select country and site to view detected species")
    st.sidebar.header("🔍 Site Selection")
    
    # Country & device selection
    countries = get_available_countries()
    selected_country = st.sidebar.selectbox("Select Country", countries)

    devices = get_sites_for_country(selected_country)
    selected_device = st.sidebar.selectbox("Select Device", devices)
    
    detected_species = get_species_for_site(selected_country, selected_device)
    selected_species = st.sidebar.selectbox("Select Species", detected_species)

    # Get and display detected species for this site
    st.subheader(f"🐦 Detected Species at {selected_country} - {selected_device}")
    
    # Find the files containing the species audio
    with st.spinner("Loading files where the species has been found..."):
        audio_files = get_audio_files_for_species(selected_country, selected_device, selected_species)

    if audio_files:
        st.write(f"**Total files where species detected:** {len(audio_files)}")
        file_df = pd.DataFrame({"Audio files": audio_files})
        file_df_full_path = get_full_file_path(file_df, selected_country, selected_device)
        st.subheader("📋 File List")
        st.dataframe(file_df_full_path, use_container_width=True, height=400)  
    else:
        st.warning(f"No species found for {selected_country} - {selected_device}")
    
    # get the full file paths for the audio clips
    

    # Get the detection audio clip


    # Show a random clip


    # Have a way for the user to annotate the clip like:
    # Is it the correct species? Yes/No, if no, what species is it?


if __name__ == "__main__":
    main()