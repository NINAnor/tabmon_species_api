"""
Pro Mode Session Management

This module handles session state for Pro mode.
"""

import streamlit as st

from queries import get_random_assigned_clip
from session_utils import init_base_session, init_state_vars, check_params_changed


def initialize_pro_session():
    """Initialize Pro mode session state variables."""
    init_base_session()
    init_state_vars('pro_', {
        'current_clip': None,
        'clip_params': None,
        'authenticated': False,
        'user_id': None
    })


def clear_pro_clip_state():
    """Clear all Pro mode clip-related state variables."""
    st.session_state.pro_current_clip = None
    st.session_state.pro_clip_params = None


def get_or_load_pro_clip(selections):
    """
    Get the current Pro mode clip or load a new one if needed.
    
    Args:
        selections: Dictionary containing user selections
        
    Returns:
        Dictionary containing clip information or None if no clips available
    """
    initialize_pro_session()
    
    current_params = (selections["user_id"], selections["confidence_threshold"])

    # If parameters changed, clear and reload
    if check_params_changed('pro_clip_params', current_params):
        clear_pro_clip_state()
        st.session_state.pro_clip_params = current_params

    # If current clip is None, get new clip
    if st.session_state.pro_current_clip is None:
        st.session_state.pro_current_clip = get_random_assigned_clip(
            selections["user_id"],
            selections["confidence_threshold"],
        )

    return st.session_state.pro_current_clip
