"""
Session Management for the TABMON Listening Lab dashboard.

This module handles session state initialization, clip queue management,
and clip loading logic for normal mode.
"""

import streamlit as st

from shared.queries import get_random_detection_clip
from shared.session_utils import init_base_session, init_state_vars, check_params_changed


def initialize_session():
    """Initialize session state variables if they don't exist."""
    init_base_session()
    init_state_vars('', {
        'current_clip': None,
        'clip_params': None,
        'clip_queue': [],
        'species_initialized': False
    })


def clear_clip_state():
    """Clear all clip-related state variables."""
    for key in ['current_clip', 'clip_params', 'clip_queue']:
        st.session_state[key] = None if key != 'clip_queue' else []


def get_or_load_clip(selections):
    """
    Get the current clip or load a new one if needed.
    
    Args:
        selections: Dictionary containing user selections
        
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

    # If parameters changed, clear and reload
    if check_params_changed('clip_params', current_params):
        clear_clip_state()
        st.session_state.clip_params = current_params

    # If current clip is None, get new clip
    if st.session_state.current_clip is None:
        st.session_state.current_clip = get_random_detection_clip(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )

    return st.session_state.current_clip
