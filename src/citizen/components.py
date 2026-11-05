"""Citizen Mode UI Components."""

import streamlit as st

from citizen.queries import get_audio_url, get_validated_clips
from citizen.session import load_next_clip
from config import CITIZEN_CONTEXT_SECONDS
from core.audio import (
    extract_clip,
    format_detection_time,
    render_audio_player,
    render_spectrogram,
)
from core.ui import render_context_slider


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
2. **Listen** to the audio clip that appears (3 seconds of detection, plus
   adjustable context before and after)
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

    with st.expander("⌨️ Keyboard Shortcuts", expanded=False):
        st.markdown("""
- **y / n / u** — Answer: Yes / No / Unsure
- **q / w / e** — Confidence: Low / Moderate / High
- **Space** — Submit the validation
- **d** — Load a new detection
""")


def render_clip_section(result, selections):
    """Render the audio clip section with player and metadata.

    Returns True if a clip was loaded, False otherwise.
    """
    if not result:
        st.warning(
            f"No clips found for {selections['species_display']} at "
            f"{selections['site_name']}"
        )
        return False

    if result.get("all_validated"):
        st.success(
            f"🎉 Congratulations! All {result['total_clips']} clips for "
            f"{selections['species_display']} at {selections['site_name']} "
            f"above the confidence threshold of {selections['confidence_threshold']} "
            f"have been validated!"
        )
        st.info(
            "✅ This species/location combination is complete. "
            "Try selecting a different species or location."
        )
        st.balloons()
        return False

    with st.container(border=True):
        st.markdown("### 🎵 Audio Clip")

        context_seconds = render_context_slider(
            "context_seconds", default=CITIZEN_CONTEXT_SECONDS
        )
        with st.spinner("Loading audio clip..."):
            audio_url = get_audio_url(result["filename"], selections["country"])
            clip = extract_clip(audio_url, result["start_time"], context_seconds)

        detection_time = format_detection_time(result["filename"], result["start_time"])
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**📍 Site:** `{selections['site_name']}`")
            st.markdown(f"**📁 File:** `{result['filename']}`")
        with col2:
            if detection_time:
                st.markdown(f"**🔊 Detection time:** `{detection_time}`")
            st.markdown(
                f"**🎯 BirdNET Confidence score:** `{result['confidence']:.2f}`"
            )

        render_audio_player(clip)

        with st.expander("📊 Show Spectrogram", expanded=True):
            render_spectrogram(audio_url, result["start_time"], context_seconds)

        if st.button(
            "🔄 Load New Detection",
            help="Get a new random detection for the same species and location",
            shortcut="d",
        ):
            load_next_clip()
            get_validated_clips.clear()
            st.rerun()

    return True


def render_empty_validation_placeholder():
    """Render placeholder message when no validation form should be shown."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        st.info("Select your parameters and an audio clip will appear for validation.")


def render_all_validated_placeholder():
    """Render message when all clips have been validated."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        st.success("All clips validated for this combination!")
