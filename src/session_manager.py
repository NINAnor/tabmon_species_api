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
        'user_id': None,
        'validated_clips_session': set(),  # Track validated clips in current session
        'remaining_count': None  # Cache remaining count to avoid repeated queries
    })


def clear_pro_clip_state():
    """Clear all Pro mode clip-related state variables."""
    st.session_state.pro_current_clip = None
    st.session_state.pro_clip_params = None
    st.session_state.pro_validated_clips_session = set()  # Clear session validations
    st.session_state.pro_remaining_count = None  # Reset remaining count
    
    # Clear caches when switching users to avoid showing wrong data
    from queries import get_validated_pro_clips, get_remaining_pro_clips_count
    get_validated_pro_clips.clear()
    get_remaining_pro_clips_count.clear()


def get_or_load_pro_clip(selections):
    """
    Get the current Pro mode clip or load a new one if needed.
    
    Args:
        selections: Dictionary containing user selections (user_id, language_code)
        
    Returns:
        Dictionary containing clip information or None if no clips available
    """
    initialize_pro_session()
    
    # Include language_code in params to detect language changes
    current_params = (selections["user_id"], selections.get("language_code", "Scientific_Name"))

    # If parameters changed, clear and reload
    if check_params_changed('pro_clip_params', current_params):
        clear_pro_clip_state()
        st.session_state.pro_clip_params = current_params

    # If current clip is None, get new clip
    if st.session_state.pro_current_clip is None:
        # Initialize remaining count on first load
        if st.session_state.pro_remaining_count is None:
            from queries import get_remaining_pro_clips_count
            st.session_state.pro_remaining_count = get_remaining_pro_clips_count(selections["user_id"])
        
        st.session_state.pro_current_clip = get_random_assigned_clip(
            selections["user_id"],
        )

    return st.session_state.pro_current_clip
