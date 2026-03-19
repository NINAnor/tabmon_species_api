"""Session Management for the TABMON Listening Lab dashboard."""

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


def get_or_load_clip(selections):
    """Get the current clip or load a new one if needed."""
    initialize_session()

    current_params = (
        selections["country"],
        selections["device"],
        selections["species"],
        selections["confidence_threshold"],
    )

    # If parameters changed, reload
    if st.session_state.clip_params != current_params:
        st.session_state.clip_params = current_params
        st.session_state.current_clip = None

    # Only fetch a new clip when we don't already have one
    if st.session_state.current_clip is None:
        st.session_state.current_clip = get_random_detection_clip(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )

    return st.session_state.current_clip
