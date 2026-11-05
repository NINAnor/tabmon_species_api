"""Citizen Mode Selection Handlers: country, site, datetime range, species
and confidence threshold in the sidebar."""

import random
from datetime import datetime

import streamlit as st

from citizen.queries import get_datetime_bounds_for_site, get_species_for_site
from core.sites import get_available_countries, get_sites_for_country
from core.species import get_species_display_names
from core.ui import render_language_selector, render_sidebar_logo

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


def _randomize_initial_selection(countries):
    """On first load, pick a random country, site and (preferably common)
    species, so they are consistent before any selectbox renders."""
    country = random.choice(countries)
    st.session_state.country_selector = country

    sites = get_sites_for_country(country)
    if not sites:
        return
    site_name = random.choice(list(sites))
    st.session_state.site_selector = site_name

    species = get_species_for_site(country, sites[site_name])
    if species:
        common_species = [s for s in COMMON_SPECIES if s in species]
        st.session_state.species_selector = random.choice(common_species or species)


def _render_datetime_filter(country, device_id):
    """Optional datetime range filter.

    Returns (start_datetime, end_datetime), each possibly None, or None if
    the range is invalid.
    """
    if not st.sidebar.checkbox(
        "Filter by datetime",
        value=False,
        help="Restrict detections to a specific recording datetime range",
    ):
        return None, None

    min_datetime, max_datetime = get_datetime_bounds_for_site(country, device_id)
    if not (min_datetime and max_datetime):
        st.sidebar.warning(
            "No recording datetime metadata found for this site. "
            "Datetime filtering has been disabled."
        )
        return None, None

    st.sidebar.caption(
        "Available range: "
        f"{min_datetime:%Y-%m-%d %H:%M:%S} to "
        f"{max_datetime:%Y-%m-%d %H:%M:%S}"
    )
    start_date = st.sidebar.date_input(
        "Start date", value=min_datetime.date(), key="start_date_filter"
    )
    start_time = st.sidebar.time_input(
        "Start time",
        value=min_datetime.time().replace(microsecond=0),
        key="start_time_filter",
    )
    end_date = st.sidebar.date_input(
        "End date", value=max_datetime.date(), key="end_date_filter"
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
    return start_datetime, end_datetime


def get_user_selections():
    """Render sidebar controls and collect user selections.

    Returns a dict (country, site_name, device, species, species_display,
    language_code, confidence_threshold, start_datetime, end_datetime),
    or None when there is nothing to show.
    """
    render_sidebar_logo()
    st.sidebar.header("🔍 Select the parameters")

    language_code = render_language_selector()

    countries = get_available_countries()
    if "selections_initialized" not in st.session_state:
        _randomize_initial_selection(countries)
        st.session_state.selections_initialized = True

    country = st.sidebar.selectbox("Select Country", countries, key="country_selector")

    with st.spinner(f"Loading sites for {country}..."):
        sites = get_sites_for_country(country)

    site_name = st.sidebar.selectbox("Select Site", list(sites), key="site_selector")
    device_id = sites[site_name]

    datetime_range = _render_datetime_filter(country, device_id)
    if datetime_range is None:
        return None
    start_datetime, end_datetime = datetime_range

    detected_species = get_species_for_site(
        country, device_id, start_datetime, end_datetime
    )
    if not detected_species:
        st.sidebar.warning(
            f"⚠️ No data available for site '{site_name}'. "
            "Please select a different site."
        )
        return None

    species_display_map = get_species_display_names(detected_species, language_code)
    species_display = st.sidebar.selectbox(
        "Select Species", list(species_display_map), key="species_selector"
    )

    confidence_threshold = st.sidebar.slider(
        "Minimum Confidence Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.8,
        step=0.05,
        help="Only show clips with BirdNET confidence above this threshold",
    )

    return {
        "language_code": language_code,
        "country": country,
        "site_name": site_name,
        "device": device_id,
        "species": species_display_map[species_display],
        "species_display": species_display,
        "confidence_threshold": confidence_threshold,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }
