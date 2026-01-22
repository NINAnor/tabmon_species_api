"""
UI Components for the TABMON Listening Lab dashboard.

This module contains UI rendering functions for normal mode.
"""

import streamlit as st

from shared.ui_utils import (
    setup_page_config,
    render_sidebar_logo,
    render_spectrogram,
    render_audio_player,
    render_clip_metadata,
    render_all_validated_message,
    clear_cache_functions,
)

def render_page_header():
    """Render the main page header and welcome message."""
    st.title("🐦 TABMON Listening Lab", text_alignment="center")

    st.markdown(
        "### Welcome to the TABMON Species Validation Tool! 🎧", text_alignment="center"
    )
    st.markdown(
        "Help us improve bird species detection by listening to audio clips and "
        "confirming what you hear. "
        "Please wait a minute or two for the application to initialize!"
        " **If you want more information about this project, check out our**"
        " **[website](https://tabmon-eu.nina.no/), our [dashboard](https://tabmon.nina.no/),"
        " and the [GitHub repository](https://github.com/NINAnor/tabmon_species_api).**",
        text_alignment="center",
    )


def render_help_section():
    """Render help information in a collapsible section."""
    with st.expander("ℹ️ Help & Instructions", expanded=False):
        st.markdown("""### 📖 How to use this tool

**Simple 4-step process:**
1. **Select your preferences** in the sidebar (you can select the country,
   location, species, and language for the name of the species)
2. **Listen** to the 9-second audio clip that appears
3. **Answer** whether you hear the selected species or not
4. **Rate your confidence** in your answer and submit!

**Your contributions help us:**
- ✅ Improve automatic bird detection models
- 🎯 Identify which species are harder to detect
- 🌍 Build better tools for biodiversity monitoring
""")

        st.markdown("""### ⏱️ What to expect

- **First load:** May take up to a minute as we process the data
- **Changing country/location/species:** Takes a few seconds to load new data
- **Languages:** Switch freely between scientific and common names in your
  language of preference!
""")


def render_clip_section(result, selections):
    """
    Render the audio clip section with player and metadata.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
        
    Returns:
        bool: True if clip was loaded successfully, False otherwise
    """
    from shared.utils import extract_clip, get_single_file_path

    if not result:
        st.warning(
            f"No clips found for {selections['species_display']} at "
            f"{selections['site_name']}"
        )
        return False

    # Check if all clips have been validated
    if result.get("all_validated"):
        render_all_validated_message(
            mode_name=f"clips for {selections['species_display']} at {selections['site_name']}",
            total_clips=result['total_clips'],
            extra_message="This species/location combination is complete. Try selecting a different species or location."
        )
        return False

    with st.container(border=True):
        st.markdown("### 🎵 Audio Clip")

        with st.spinner("Loading audio clip..."):
            full_path = get_single_file_path(
                result["filename"], selections["country"], selections["device"]
            )
            clip = extract_clip(full_path, result["start_time"])

        render_clip_metadata(result["filename"], result["confidence"])
        render_audio_player(clip)
        render_spectrogram(clip, expanded=False)

        render_load_new_button()

    return True


def render_load_new_button():
    """Render the load new detection button."""
    if st.button("🔄 Load New Detection", help="Get a new random detection for the same species and location"):
        st.session_state.current_clip = None
        st.session_state.clip_params = None
        st.session_state.clip_queue = []
        
        from shared.queries import get_all_clips_for_species, get_validated_clips
        clear_cache_functions(get_validated_clips, get_all_clips_for_species)
        st.rerun()


def render_empty_validation_placeholder():
    """Render placeholder message when no validation form should be shown."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        st.info(
            "Select your parameters and an audio clip will appear for "
            "validation."
        )


def render_all_validated_placeholder():
    """Render message when all clips have been validated."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        st.success("All clips validated for this combination!")
