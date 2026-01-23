"""
TABMON Listening Lab - Expert Mode Application

This is the main entry point for the TABMON Expert Species Validation Tool.
Expert mode is designed for assigned annotation tasks with user authentication.
"""

import streamlit as st

from handlers.selection_handlers import get_pro_user_selections
from handlers.validation_handlers import render_pro_validation_form
from session.session_manager import get_or_load_pro_clip, initialize_pro_session
from ui.ui_components import (
    render_pro_all_validated_placeholder,
    render_pro_clip_section,
    render_pro_empty_validation_placeholder,
    render_pro_help_section,
    render_pro_page_header,
)
from ui.ui_utils import setup_page_config


def main():
    """Main application entry point for Expert mode."""

    # Setup page configuration
    setup_page_config()

    # Initialize Expert session
    initialize_pro_session()
    render_pro_page_header()
    render_pro_help_section()

    # Get user selections (includes authentication)
    selections = get_pro_user_selections()

    # If not authenticated, show placeholder and return
    if selections is None:
        st.markdown("---")
        col1, col2 = st.columns([1, 1])
        with col1:
            st.container(border=True).info("🔐 Please authenticate to view clips")
        with col2:
            render_pro_empty_validation_placeholder()
        return

    st.markdown("---")

    # Load clip based on selections
    result = get_or_load_pro_clip(selections)

    # Main content: Audio Clip and Validation side by side
    col1, col2 = st.columns([1, 1])

    with col1:
        clip_loaded = render_pro_clip_section(result, selections)

    with col2:
        if result and not result.get("all_validated") and clip_loaded:
            render_pro_validation_form(result, selections)
        elif result and result.get("all_validated"):
            render_pro_all_validated_placeholder()
        else:
            render_pro_empty_validation_placeholder()


if __name__ == "__main__":
    main()
