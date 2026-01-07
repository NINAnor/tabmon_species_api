from pathlib import Path

import pandas as pd
import streamlit as st

from config import LANGUAGE_MAPPING, SITE_INFO_PATH
from queries import (
    get_available_countries,
    get_random_detection_clip,
    get_remaining_clips_count,
    get_sites_for_country,
    get_species_for_site,
)
from utils import (
    extract_clip,
    get_single_file_path,
    get_species_display_names,
    match_device_id_to_site,
    save_validation_response,
    get_validated_clips,
)


def setup_page_config():
    st.set_page_config(
        page_title="TABMON Listening Lab",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🐦",
    )


def render_page_header():
    st.title("🐦 TABMON Listening Lab")

    st.markdown("### Welcome to the TABMON Species Validation Tool! 🎧")
    st.markdown(
        "Help us improve bird species detection by listening to audio clips and confirming what you hear." \
        "Please wait a minute or two for the application to initialize!"
    )


def render_sidebar_logo():
    logo_path = Path("/app/assets/tabmon_logo.png")
    if logo_path.exists():
        st.sidebar.image(logo_path, width=300)
        st.sidebar.markdown("---")


def get_user_selections():
    render_sidebar_logo()
    st.sidebar.header("🔍 Select the parameters")

    # Language selector
    selected_language = st.sidebar.selectbox(
        "Species Name Language",
        options=["Scientific Names"] + list(LANGUAGE_MAPPING.keys()),
        help="Choose the language for species names",
    )

    # Country selection
    countries = get_available_countries()
    selected_country = st.sidebar.selectbox("Select Country", countries)

    # Site selection
    devices = get_sites_for_country(selected_country)
    device_site_mapping = match_device_id_to_site(SITE_INFO_PATH)

    filtered_sites = {}
    for device in devices:
        if device in device_site_mapping:
            site_name = device_site_mapping[device]
            filtered_sites[site_name] = device

    selected_site_name = st.sidebar.selectbox(
        "Select Site", list(filtered_sites.keys())
    )
    selected_device = filtered_sites[selected_site_name]

    # Species selection with translation
    detected_species = get_species_for_site(selected_country, selected_device)

    if selected_language == "Scientific Names":
        species_display_map = {species: species for species in detected_species}
    else:
        language_code = LANGUAGE_MAPPING[selected_language]
        species_display_map = get_species_display_names(detected_species, language_code)

    selected_species_display = st.sidebar.selectbox(
        "Select Species", list(species_display_map.keys())
    )
    selected_species = species_display_map[selected_species_display]

    # Confidence threshold
    confidence_threshold = st.sidebar.slider(
        "Minimum Confidence Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.1,
        help="Only show clips with BirdNET confidence above this threshold",
    )

    return {
        "language": selected_language,
        "country": selected_country,
        "site_name": selected_site_name,
        "device": selected_device,
        "species": selected_species,
        "species_display": selected_species_display,
        "confidence_threshold": confidence_threshold,
    }


def get_or_load_clip(selections):
    if "current_clip" not in st.session_state:
        st.session_state.current_clip = None
    if "clip_params" not in st.session_state:
        st.session_state.clip_params = None

    current_params = (
        selections["country"],
        selections["device"],
        selections["species"],
        selections["confidence_threshold"],
    )

    # Only reload clip if parameters changed or no clip exists
    if (
        st.session_state.clip_params != current_params
        or st.session_state.current_clip is None
    ):
        st.session_state.current_clip = get_random_detection_clip(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        ) # THE BOTTLENEC IS HERE AS I MAKE THE WHOLE QUERY EVERYTIME
        st.session_state.clip_params = current_params

    return st.session_state.current_clip


def render_clip_section(result, selections):
    """Render the audio clip section with player and metadata."""
    if not result:
        st.warning(
            f"No clips found for {selections['species_display']} at {selections['site_name']}"
        )
        return False

    # Check if all clips have been validated
    if result.get("all_validated"):
        st.success(
            f"🎉 Congratulations! All {result['total_clips']} clips for {selections['species_display']} at {selections['site_name']} above the confidence threshold of {selections['confidence_threshold']} have been validated!"
        )
        st.info(
            "✅ This species/location combination is complete. Try selecting a different species or location."
        )
        st.balloons()
        return False

    with st.container(border=True):
        st.markdown("### 🎵 Audio Clip")

        full_path = get_single_file_path(
            result["filename"], selections["country"], selections["device"]
        )
        clip = extract_clip(full_path, result["start_time"])

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**📁 File:** `{result['filename']}`")
        with col2:
            st.markdown(
                f"**🎯 BirdNET Confidence score:** `{result['confidence']:.2f}`"
            )

        st.audio(clip, format="audio/wav", sample_rate=48000)
        render_load_new_button()

    return True


def render_validation_form(result, selections):
    """Render the validation form and handle submission."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        st.markdown(f"**Is this detection a {selections['species_display']}?**")
        
        # Show remaining clips count
        remaining_clips = get_remaining_clips_count(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"]
        )
        if remaining_clips > 0:
            st.info(f"📊 Still **{remaining_clips}** clips to annotate for your current parameters")
        else:
            st.success("🎉 All clips validated for these parameters!")

        with st.form("validation_form"):
            validation_response = st.radio(
                "Your answer:",
                options=["Yes", "No", "Unsure"],
                index=None,
                horizontal=True,
                help="Help us validate the accuracy of our species detection models!",
            )

            user_confidence = st.radio(
                "**How confident are you in your answer?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                help="Rate your confidence in the validation above",
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation", type="primary", use_container_width=True
            )

        if submitted and validation_response and user_confidence:
            validation_data = {
                "filename": result["filename"],
                "country": selections["country"],
                "site": selections["site_name"],
                "device_id": selections["device"],
                "species": selections["species"],
                "start_time": result["start_time"],
                "confidence": result["confidence"],
                "validation_response": validation_response,
                "user_confidence": user_confidence,
                "timestamp": pd.Timestamp.now(),
            }

            save_validation_response(validation_data)
            
            # Clear session state to load a new clip automatically
            st.session_state.current_clip = None
            st.session_state.clip_params = None
            
            # Clear validation cache to ensure fresh random selection
            from queries import get_validated_clips
            get_validated_clips.clear()
            
            st.success("✅ Thank you for your time and effort!")
            st.rerun() 
        elif submitted and (not validation_response or not user_confidence):
            st.error("Please answer both questions before submitting.")

@st.cache_data
def render_explanations_section():
    with st.expander("📖 How to use this tool", expanded=True):
        st.markdown("""
        **Simple 4-step process:**
        1. **Select your preferences** in the sidebar (country, location, species, language for the name of the species)
        2. **Listen** to the 3-second audio clip that appears
        3. **Answer** whether you hear the selected species or not
        4. **Rate your confidence** in your answer and submit

        **Your contributions help us:**
        - ✅ Improve automatic bird detection models
        - 🎯 Identify which species are harder to detect
        - 🌍 Build better tools for biodiversity monitoring
        """)

    with st.expander("⏱️ What to expect", expanded=True):
        st.markdown("""
        - **First load:** May take up to a minute as we process the data
        - **Changing country/location:** Takes a few seconds to load new data
        - **New species or clips:** Nearly instant!
        - **Languages:** Switch freely between scientific and common names
        """)

st.markdown("---")


def render_load_new_button():
    """Render the load new detection button."""
    if st.button(
        "🔄 Load New Detection",
        help="Get a new random detection for the same species and location",
    ):
        st.session_state.current_clip = None
        st.session_state.clip_params = None
        from queries import get_validated_clips

        get_validated_clips.clear()
        st.rerun()


def main():
    if "session_id" not in st.session_state:
        import uuid

        st.session_state.session_id = str(uuid.uuid4())[:8]

    setup_page_config()
    render_page_header()

    selections = get_user_selections()

    col1, col2 = st.columns([1, 1])

    with col1:
        render_explanations_section()

    with col2:
        result = get_or_load_clip(selections)
        clip_loaded = render_clip_section(result, selections)

        st.markdown("")

        if result and not result.get("all_validated") and clip_loaded:
            render_validation_form(result, selections)
        else:
            with st.container(border=True):
                st.markdown("### 🎯 Validation")
                if not result or not clip_loaded:
                    st.info(
                        "Select your parameters and an audio clip will appear for validation."
                    )
                elif result.get("all_validated"):
                    st.success("All clips validated for this combination!")


if __name__ == "__main__":
    main()
