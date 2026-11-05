"""Page layout helpers shared by both apps."""

import streamlit as st

from config import LANGUAGE_MAPPING, LOGO_PATH
from core.species import SCIENTIFIC_NAME


def setup_page_config():
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="TABMON Listening Lab",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🐦",
    )


def render_sidebar_logo():
    """Render the TABMON logo in the sidebar."""
    if LOGO_PATH.exists():
        st.sidebar.image(LOGO_PATH, width=300)
        st.sidebar.markdown("---")


def render_language_selector():
    """Render the species-name language selector in the sidebar.

    Returns the language code (column of the multilingual species CSV).
    """
    selected_language = st.sidebar.selectbox(
        "Species Name Language",
        options=["Scientific Names", *LANGUAGE_MAPPING.keys()],
        help="Choose the language for species names",
    )
    return LANGUAGE_MAPPING.get(selected_language, SCIENTIFIC_NAME)


def render_context_slider(key, default=1):
    """Slider for the seconds of audio played around the detection."""
    return st.slider(
        "Context around detection (s)",
        min_value=1,
        max_value=5,
        value=default,
        step=1,
        key=key,
        help=(
            "Seconds of audio context before and after the 3-second "
            "BirdNET detection window."
        ),
    )
