import librosa
import streamlit as st
import pandas as pd

from config import DATA_PATH, VALIDATION_RESPONSES_PATH, BIRDNET_MULTILINGUAL_PATH

def extract_clip(audio_file_path, start_time, sr=48000):
    audio_data, _ = librosa.load(audio_file_path, sr=sr, mono=True)
    start_sample = int(start_time * sr)
    end_sample = int((start_time + 3) * sr)
    return audio_data[start_sample:end_sample]

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

def save_validation_response(validation_data):

    columns = [
        'filename', 'country', 'site', 'device_id', 'species', 
        'start_time', 'confidence', 'validation_response', 'user_confidence', 'timestamp'
    ]
    
    validation_df = pd.DataFrame([validation_data])
    
    if VALIDATION_RESPONSES_PATH.exists():
        validation_df.to_csv(VALIDATION_RESPONSES_PATH, mode='a', header=False, index=False)
    else:
        validation_df.to_csv(VALIDATION_RESPONSES_PATH, mode='w', header=True, index=False)
    
    get_validated_clips.clear()
    
    return True

@st.cache_data(ttl=60) 
def get_validated_clips(country, device_id, species):
    if not VALIDATION_RESPONSES_PATH.exists():
        return set()
    
    try:
        validation_df = pd.read_csv(VALIDATION_RESPONSES_PATH)
        # Filter for same country, device, and species
        filtered_df = validation_df[
            (validation_df['country'] == country) & 
            (validation_df['device_id'] == device_id) & 
            (validation_df['species'] == species)
        ]
        # Return set of filename + start_time combinations that have been validated
        validated_clips = set()
        for _, row in filtered_df.iterrows():
            validated_clips.add((row['filename'], row['start_time']))
        return validated_clips
    except Exception as e:
        return set()

def match_device_id_to_site(site_info_path):
    site_info_df = pd.read_csv(site_info_path)
    
    device_site_map = {}
    for _, row in site_info_df.iterrows():
        device_site_map[row["DeviceID"]] = row["Site"]

    return device_site_map

@st.cache_data
def load_species_translations():
    """Load the multilingual species name translations"""
    return pd.read_csv(BIRDNET_MULTILINGUAL_PATH)

def get_species_display_names(species_list, language_code):
    """Convert scientific names to display names in selected language"""
    if language_code == "Scientific_Name":
        return {species: species for species in species_list}
    
    translations_df = load_species_translations()
    species_map = {}
    
    for species in species_list:
        translation_row = translations_df[translations_df['Scientific_Name'] == species]
        if not translation_row.empty and language_code in translation_row.columns:
            translated_name = translation_row[language_code].iloc[0]
            if pd.notna(translated_name):
                species_map[translated_name] = species
            else:
                species_map[species] = species  # Fallback to scientific name
        else:
            species_map[species] = species  # Fallback to scientific name
    
    return species_map