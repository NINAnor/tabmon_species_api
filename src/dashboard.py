"""
TABMON Listening Lab - Main Dashboard Application

This is the main entry point for the TABMON Species Validation Tool.
It orchestrates the UI components, session management, and user interactions.
"""

import streamlit as st

from session_manager import initialize_session, get_or_load_clip
from selection_handlers import get_user_selections
from ui_components import (
    setup_page_config,
    render_page_header,
    render_help_section,
    render_clip_section,
    render_empty_validation_placeholder,
    render_all_validated_placeholder,
)
from validation_handlers import render_validation_form


def main():
    """Main application entry point."""
    # Initialize session state
    initialize_session()
    
    # Setup page configuration and render header
    setup_page_config()
    render_page_header()
    render_help_section()

    # Get user selections from sidebar
    selections = get_user_selections()

    st.markdown("---")

    # Load clip based on selections
    result = get_or_load_clip(selections)

    # Main content: Audio Clip and Validation side by side
    col1, col2 = st.columns([1, 1])

    with col1:
        clip_loaded = render_clip_section(result, selections)

    with col2:
        if result and not result.get("all_validated") and clip_loaded:
            render_validation_form(result, selections)
        elif result and result.get("all_validated"):
            render_all_validated_placeholder()
        else:
            render_empty_validation_placeholder()


if __name__ == "__main__":
    main()

