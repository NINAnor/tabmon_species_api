"""
Selection Handlers for the TABMON Listening Lab dashboard.

This module manages user input selections for normal mode.
"""

import random
import streamlit as st

from shared.config import LANGUAGE_MAPPING, SITE_INFO_S3_PATH
from shared.queries import (
    get_available_countries,
    get_sites_for_country,
    get_species_for_site,
)
from shared.utils import get_species_display_names, match_device_id_to_site
from shared.ui_utils import render_sidebar_logo


# Common species that are present across most habitats
COMMON_SPECIES = [
    "Corvus corone",
    "Columba palumbus",
    "Erithacus rubecula",
    "Turdus merula",
    "Cygnus olor",
    "Branta canadensis",
    "Cuculus canorus",
]


def get_user_selections():
    """
    Render sidebar controls and collect user selections.
    
    Returns:
        Dictionary containing all user selections
    """
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
    device_site_mapping = match_device_id_to_site(SITE_INFO_S3_PATH)

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

    display_names = list(species_display_map.keys())

    # Determine default species index - maintain selection across reruns
    default_index = 0

    # Clean up old session state variable if it exists
    if "selected_species_display" in st.session_state:
        del st.session_state.selected_species_display

    # If we have a previously selected species (stored as scientific name), find it
    if "selected_species_scientific" in st.session_state:
        # Find the display name for this scientific name
        for i, display_name in enumerate(display_names):
            if (
                species_display_map[display_name]
                == st.session_state.selected_species_scientific
            ):
                default_index = i
                break

    # Only do random selection on very first load
    if "species_initialized" not in st.session_state:
        available_common = [s for s in COMMON_SPECIES if s in detected_species]
        if available_common:
            random_species = random.choice(available_common)
            for i, display_name in enumerate(display_names):
                if species_display_map[display_name] == random_species:
                    default_index = i
                    break
        st.session_state.species_initialized = True

    selected_species_display = st.sidebar.selectbox(
        "Select Species", display_names, index=default_index
    )
    selected_species = species_display_map[selected_species_display]

    # Store the scientific name (not display name) for next rerun
    st.session_state.selected_species_scientific = selected_species

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
