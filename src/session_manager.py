"""
Session Management for the TABMON Listening Lab dashboard.

This module handles session state initialization, clip queue management,
and clip loading logic.
"""

import uuid

import streamlit as st

from queries import get_random_detection_clip


def initialize_session():
    """Initialize session state variables if they don't exist."""
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())[:8]

    if "current_clip" not in st.session_state:
        st.session_state.current_clip = None

    if "clip_params" not in st.session_state:
        st.session_state.clip_params = None

    if "clip_queue" not in st.session_state:
        st.session_state.clip_queue = []

    if "species_initialized" not in st.session_state:
        st.session_state.species_initialized = False


def clear_clip_state():
    """Clear all clip-related state variables."""
    st.session_state.current_clip = None
    st.session_state.clip_params = None
    st.session_state.clip_queue = []


def get_or_load_clip(selections):
    """
    Get the current clip or load a new one if needed.

    Manages clip queue and parameter tracking to avoid unnecessary reloads.

    Args:
        selections: Dictionary containing user selections
                    (country, device, species, etc.)

    Returns:
        Dictionary containing clip information or None if no clips available
    """
    initialize_session()

    current_params = (
        selections["country"],
        selections["device"],
        selections["species"],
        selections["confidence_threshold"],
    )

    # If parameters changed, clear queue and reload
    if st.session_state.clip_params != current_params:
        st.session_state.clip_queue = []
        st.session_state.clip_params = current_params
        st.session_state.current_clip = None

    # If queue is empty or current clip is None, get new clip
    if not st.session_state.clip_queue or st.session_state.current_clip is None:
        clip_result = get_random_detection_clip(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )
        st.session_state.current_clip = clip_result

    return st.session_state.current_clip
