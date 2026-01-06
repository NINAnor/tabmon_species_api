import streamlit as st
from pathlib import Path
import pandas as pd
from queries import (
    get_available_countries,
    get_sites_for_country,
    get_species_for_site,
    get_random_detection_clip
)
from config import SITE_INFO_PATH
from utils import extract_clip, get_single_file_path, match_device_id_to_site, save_validation_response

def main():

    if "session_id" not in st.session_state:
        import uuid

        st.session_state.session_id = str(uuid.uuid4())[:8]  # Short unique ID

    # Page configuration
    st.set_page_config(
        page_title="TABMON Listening Lab",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🎙️",
    )

    # Add the logo
    logo_path = Path("/app/assets/tabmon_logo.png")
    st.sidebar.image(logo_path, width=300)
    st.sidebar.markdown("---")

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
    st.markdown("The first load may take a little while as the data is being processed. **Loading a new site/country can take a little bit of time (maximum 1 minute)**. However, **loading a new species or a new detection clip is almost instantaneous!**")

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

    # Add confidence threshold selector
    confidence_threshold = st.sidebar.slider(
        "Minimum Confidence Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.1,
        help="Only show clips with BirdNET confidence above this threshold"
    )

    # Get and display detected species for this site
    st.subheader(f"A random clip has been chosen for {selected_species} in {selected_country} at {selected_site_name}")
    
    # Get a random detection from the merged prediction, matching the site and country
    result = get_random_detection_clip(selected_country, selected_device, selected_species, confidence_threshold)
    
    if not result:
        st.warning(f"No clips found for {selected_species} at {selected_site_name}")
        return
    
    # Check if all clips have been validated
    if result.get("all_validated"):
        st.success(f"🎉 Congratulations! All {result['total_clips']} clips for {selected_species} at {selected_site_name} have been validated!")
        st.info("✅ This species/location combination is complete. Try selecting a different species or location.")
        st.balloons()
        return
        
    full_path = get_single_file_path(result["filename"], selected_country, selected_device)
    clip = extract_clip(full_path, result["start_time"])

    st.markdown(f"**Listening Clip from file:** {result['filename']}  |  **BirdNET Confidence:** {result['confidence']}")
    st.audio(clip, format="audio/wav", sample_rate=48000)

    # Add validation section
    st.divider()
    st.subheader("🎯 Validation")
    st.markdown(f"**Is this detection a {selected_species}?**")
    
    # Use form to prevent reloading on each radio button click
    with st.form("validation_form"):
        validation_response = st.radio(
            "Your answer:",
            options=["Yes", "No", "Unknown"],
            index=None,
            horizontal=True,
            help="Help us validate the accuracy of our species detection models!"
        )
        
        # Add confidence level question
        user_confidence = st.radio(
            "**How confident are you in your answer?**",
            options=["Low", "Moderate", "High"],
            index=None,
            horizontal=True,
            help="Rate your confidence in the validation above"
        )
        
        # Submit button (always visible in forms)
        submitted = st.form_submit_button("✅ Submit Validation", type="primary")
        
        if submitted and validation_response and user_confidence:
            # Store validation response
            validation_data = {
                'filename': result['filename'],
                'country': selected_country,
                'site': selected_site_name,
                'device_id': selected_device,
                'species': selected_species,
                'start_time': result['start_time'],
                'confidence': result['confidence'],
                'validation_response': validation_response,
                'user_confidence': user_confidence,
                'timestamp': pd.Timestamp.now()
            }
            
            save_validation_response(validation_data)
            st.success(f"✅ Thank you for your time and effort!")
            st.balloons()
        elif submitted and (not validation_response or not user_confidence):
            st.error("Please answer both questions before submitting.")

    # Add button to load a new detection
    if st.button("🔄 Load New Detection", help="Get a new random detection for the same species and location"):
        st.rerun()

if __name__ == "__main__":
    main()