import streamlit as st
from pathlib import Path
import pandas as pd
from queries import (
    get_available_countries,
    get_sites_for_country,
    match_device_id_to_site,
    get_species_for_site,
    get_audio_files_for_species,
    get_random_detection_clip,
    SITE_INFO_PATH,
    DATA_PATH
)

from utils import extract_clip

def get_single_file_path(filename, country, deployment_id):
    """Get the full path for a single audio file."""
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

    if proj_path.exists():
        device_dirs = list(proj_path.glob(f"bugg_RPiID-*{deviceID}"))
        if device_dirs:
            device_path = device_dirs[0]
            possible_files = list(device_path.glob(f"*/{filename}"))
            if possible_files:
                return str(possible_files[0])
    return "File not found"

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
        page_title="TABMON Listening Lab",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🎙️",
    )
    
    st.title("🎙️ TABMON Listening Lab")
    st.markdown("This application makes it possible to explore our TABMON audio dataset and **listen to detected species clips.**")
    st.markdown("The detections are based on automated species identification models (i.e. BirdNET 2.4) and **may not always be accurate.**")
    st.markdown("Please use this tool to help validate and improve our models by listening to the clips and providing feedback!")
    
    st.sidebar.header("🔍 Select the parameters")

    # Country & device selection
    countries = get_available_countries()
    selected_country = st.sidebar.selectbox("Select Country", countries)

    devices = get_sites_for_country(selected_country)
    device_site_mapping = match_device_id_to_site(SITE_INFO_PATH)
    
    # FAILSAFE - Maybe not necessary?
    # Create filtered sites dictionary - only devices that have site mapping
    filtered_sites = {}
    for device in devices:
        if device in device_site_mapping:
            site_name = device_site_mapping[device]
            filtered_sites[site_name] = device

    # Use site names in selectbox
    selected_site_name = st.sidebar.selectbox("Select Site", list(filtered_sites.keys()))
    selected_device = filtered_sites[selected_site_name]
    
    detected_species = get_species_for_site(selected_country, selected_device)
    selected_species = st.sidebar.selectbox("Select Species", detected_species)

    # Get and display detected species for this site
    st.subheader(f"A random clip has been chosen for {selected_species} in {selected_country} at {selected_site_name}")
    
    # Find the files containing the species audio
    #with st.spinner("Loading files where the species has been found..."):
    #    audio_files = get_audio_files_for_species(selected_country, selected_device, selected_species)

    #if audio_files:
    #    st.write(f"**Total files where species detected:** {len(audio_files)}")
    #    file_df = pd.DataFrame({"Audio files": audio_files})
    #    file_df_full_path = get_full_file_path(file_df, selected_country, selected_device)
    #    st.subheader("📋 File List")
    #    st.dataframe(file_df_full_path, use_container_width=True, height=400)  
    #else:
    #    st.warning(f"No species found for {selected_country} - {selected_device}")
    
    # Get a random detection from the merged prediction, matching the site and country
    result = get_random_detection_clip(selected_country, selected_device, selected_species)
    full_path = get_single_file_path(result["filename"], selected_country, selected_device)
    clip = extract_clip(full_path, result["start_time"])

    st.markdown(f"**Listening Clip from file:** {result['filename']}  |  **BirdNET Confidence:** {result['confidence']}")
    st.audio(clip, format="audio/wav", sample_rate=48000)



    # Have a way for the user to annotate the clip like:
    # Is it the correct species? Yes/No, if no, what species is it?


if __name__ == "__main__":
    main()