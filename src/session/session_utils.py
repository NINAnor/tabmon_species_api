"""Shared Session Management Utilities."""

import uuid
import streamlit as st


def init_base_session():
    """Initialize base session state variables."""
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())[:8]


def init_state_vars(prefix, vars_dict):
    """Initialize multiple state variables with a prefix."""
    for var_name, default_value in vars_dict.items():
        full_name = f"{prefix}{var_name}" if prefix else var_name
        if full_name not in st.session_state:
            st.session_state[full_name] = default_value


def clear_state_vars(prefix, var_names):
    """Clear multiple state variables."""
    for var_name in var_names:
        full_name = f"{prefix}{var_name}" if prefix else var_name
        if full_name in st.session_state:
            st.session_state[full_name] = None


def check_params_changed(state_key, new_params):
    """Check if parameters have changed. Returns True if changed."""
    if state_key not in st.session_state:
        return True
    return st.session_state[state_key] != new_params
