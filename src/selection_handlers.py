"""
Selection Handlers for the TABMON Listening Lab dashboard.

This module manages user input selections including country, site, species,
language, and confidence threshold selections in the sidebar.
"""

import random
from datetime import datetime

import streamlit as st

from config import LANGUAGE_MAPPING, SITE_INFO_S3_PATH
from queries import (
    get_available_countries,
    get_datetime_bounds_for_site,
    get_sites_for_country,
    get_species_for_site,
)
from utils import get_species_display_names, match_device_id_to_site

# Common species that are present across most habitats
COMMON_SPECIES = [
    "Phylloscopus collybita",
    "Erithacus rubecula",
    "Phylloscopus trochilus",
    "Fringilla coelebs",
    "Emberiza schoeniclus",
    "Turdus merula",
    "Regulus regulus",
    "Prunella modularis",
    "Troglodytes troglodytes",
    "Motacilla alba",
]


def get_user_selections():
    """
    Render sidebar controls and collect user selections.

    Returns:
        Dictionary containing all user selections:
        - language: Selected language for species names
        - country: Selected country
        - site_name: Human-readable site name
        - device: Device ID for the selected site
        - species: Scientific name of selected species
        - species_display: Display name of selected species (in chosen language)
        - confidence_threshold: Minimum confidence threshold
    """
    from ui_components import render_sidebar_logo

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
    device_site_mapping = match_device_id_to_site(SITE_INFO_S3_PATH)

    # On very first load, randomize country, site, and species together
    # so they are all consistent before any selectbox renders.
    if "selections_initialized" not in st.session_state:
        random_country = random.choice(countries)  # noqa: S311
        st.session_state.country_selector = random_country

        # Pre-compute site for the random country
        init_devices = get_sites_for_country(random_country)
        init_sites = {}
        for d in init_devices:
            if d in device_site_mapping:
                init_sites[device_site_mapping[d]] = d
        init_site_names = list(init_sites.keys())
        if init_site_names:
            random_site = random.choice(init_site_names)  # noqa: S311
            st.session_state.site_selector = random_site

            # Pre-compute species for the random site
            init_device = init_sites[random_site]
            init_species = get_species_for_site(random_country, init_device)
            if init_species:
                available_common = [s for s in COMMON_SPECIES if s in init_species]
                if available_common:
                    rand_sp = random.choice(available_common)  # noqa: S311
                else:
                    rand_sp = random.choice(init_species)  # noqa: S311
                st.session_state.species_selector = rand_sp

        st.session_state.selections_initialized = True

    selected_country = st.sidebar.selectbox(
        "Select Country", countries, key="country_selector"
    )

    # Site selection
    with st.spinner(f"Loading sites for {selected_country}..."):
        devices = get_sites_for_country(selected_country)

    filtered_sites = {}
    for device in devices:
        if device in device_site_mapping:
            site_name = device_site_mapping[device]
            filtered_sites[site_name] = device

    site_names = list(filtered_sites.keys())

    selected_site_name = st.sidebar.selectbox(
        "Select Site", site_names, key="site_selector"
    )
    selected_device = filtered_sites[selected_site_name]

    # Optional datetime filter
    use_datetime_filter = st.sidebar.checkbox(
        "Filter by datetime",
        value=False,
        help="Restrict detections to a specific recording datetime range",
    )

    start_datetime = None
    end_datetime = None

    if use_datetime_filter:
        min_datetime, max_datetime = get_datetime_bounds_for_site(
            selected_country, selected_device
        )

        if min_datetime and max_datetime:
            st.sidebar.caption(
                "Available range: "
                f"{min_datetime:%Y-%m-%d %H:%M:%S} to "
                f"{max_datetime:%Y-%m-%d %H:%M:%S}"
            )

            start_date = st.sidebar.date_input(
                "Start date",
                value=min_datetime.date(),
                key="start_date_filter",
            )
            start_time = st.sidebar.time_input(
                "Start time",
                value=min_datetime.time().replace(microsecond=0),
                key="start_time_filter",
            )
            end_date = st.sidebar.date_input(
                "End date",
                value=max_datetime.date(),
                key="end_date_filter",
            )
            end_time = st.sidebar.time_input(
                "End time",
                value=max_datetime.time().replace(microsecond=0),
                key="end_time_filter",
            )

            start_datetime = datetime.combine(start_date, start_time)
            end_datetime = datetime.combine(end_date, end_time)

            if start_datetime > end_datetime:
                st.sidebar.error("Start datetime must be earlier than end datetime")
                return None
        else:
            st.sidebar.warning(
                "No recording datetime metadata found for this site. "
                "Datetime filtering has been disabled."
            )
            use_datetime_filter = False

    # Species selection with translation
    detected_species = get_species_for_site(
        selected_country,
        selected_device,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )

    # Check if site has data
    if not detected_species:
        st.sidebar.warning(
            f"⚠️ No data available for site '{selected_site_name}'. "
            "Please select a different site."
        )
        return None

    if selected_language == "Scientific Names":
        species_display_map = {species: species for species in detected_species}
    else:
        language_code = LANGUAGE_MAPPING[selected_language]
        species_display_map = get_species_display_names(detected_species, language_code)

    display_names = list(species_display_map.keys())

    # Clean up old session state variable if it exists
    if "selected_species_display" in st.session_state:
        del st.session_state.selected_species_display

    selected_species_display = st.sidebar.selectbox(
        "Select Species", display_names, key="species_selector"
    )
    selected_species = species_display_map[selected_species_display]

    # Confidence threshold
    confidence_threshold = st.sidebar.slider(
        "Minimum Confidence Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.8,
        step=0.05,
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
        "use_datetime_filter": use_datetime_filter,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }
