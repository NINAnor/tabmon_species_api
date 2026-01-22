"""
TABMON Listening Lab - Main Dashboard Application

This is the main entry point for the TABMON Species Validation Tool.
It orchestrates the UI components, session management, and user interactions.
Supports both Normal mode (random validation) and Pro mode (assigned annotations).
"""

import streamlit as st

from normal.session_manager import initialize_session, get_or_load_clip
from normal.selection_handlers import get_user_selections
from normal.ui_components import (
    setup_page_config,
    render_page_header,
    render_help_section,
    render_clip_section,
    render_empty_validation_placeholder,
    render_all_validated_placeholder,
)
from normal.validation_handlers import render_validation_form


def run_normal_mode():
    """Run the application in Normal mode."""
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


def run_pro_mode():
    """Run the application in Pro mode."""
    from pro.session_manager import initialize_pro_session, get_or_load_pro_clip
    from pro.selection_handlers import get_pro_user_selections
    from pro.ui_components import (
        render_pro_page_header,
        render_pro_help_section,
        render_pro_clip_section,
        render_pro_empty_validation_placeholder,
        render_pro_all_validated_placeholder,
    )
    from pro.validation_handlers import render_pro_validation_form
    from pro.queries import get_top_species_for_database
    
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
    
    # Get top species for checklist
    top_species = get_top_species_for_database()

    # Main content: Audio Clip and Validation side by side
    col1, col2 = st.columns([1, 1])

    with col1:
        clip_loaded = render_pro_clip_section(result, selections)

    with col2:
        if result and not result.get("all_validated") and clip_loaded:
            render_pro_validation_form(result, selections, top_species)
        elif result and result.get("all_validated"):
            render_pro_all_validated_placeholder()
        else:
            render_pro_empty_validation_placeholder()


def main():
    """Main application entry point with mode selection."""
    # Initialize session state
    initialize_session()
    
    # Setup page configuration
    setup_page_config()
    
    # Mode selection in sidebar
    if "app_mode" not in st.session_state:
        st.session_state.app_mode = "normal"
    
    # Add mode selector at the top of sidebar
    st.sidebar.markdown("### 🔀 Application Mode")
    mode = st.sidebar.radio(
        "Select Mode:",
        ["Normal", "Pro"],
        index=0 if st.session_state.app_mode == "normal" else 1,
        help="Normal: Random clip validation | Pro: Assigned annotation tasks"
    )
    
    # Update mode if changed
    new_mode = mode.lower()
    if new_mode != st.session_state.app_mode:
        st.session_state.app_mode = new_mode
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Route to appropriate mode
    if st.session_state.app_mode == "pro":
        run_pro_mode()
    else:
        run_normal_mode()


if __name__ == "__main__":
    main()


