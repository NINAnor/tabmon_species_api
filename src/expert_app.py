"""
TABMON Listening Lab - Expert app.

Annotation of assigned clips (user ID login), plus review/cross-validation
of previous annotations. Used for the TABMON studies.
"""

import streamlit as st

from core.ui import setup_page_config
from expert.components import (
    render_pro_all_validated_placeholder,
    render_pro_clip_section,
    render_pro_empty_validation_placeholder,
    render_pro_help_section,
    render_pro_page_header,
)
from expert.review_components import render_review_clip_section
from expert.review_filters import render_review_filters
from expert.selection import get_pro_user_selections
from expert.session import get_or_load_pro_clip, initialize_pro_session
from expert.validation import render_pro_validation_form


def main():
    """Main application entry point for Expert mode."""
    setup_page_config()
    initialize_pro_session()
    render_pro_page_header()
    render_pro_help_section()

    # Get user selections (includes authentication)
    selections = get_pro_user_selections()

    if selections is None:
        st.markdown("---")
        col1, col2 = st.columns([1, 1])
        with col1:
            st.container(border=True).info("🔐 Please authenticate to view clips")
        with col2:
            render_pro_empty_validation_placeholder()
        return

    st.markdown("---")

    if st.session_state.get("review_mode_active", False):
        _render_review_mode(selections)
    else:
        _render_validate_mode(selections)


def _render_validate_mode(selections):
    """Render the standard validation mode."""
    result = get_or_load_pro_clip(selections)

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


def _render_review_mode(selections):
    """Render the review/cross-checking mode."""
    has_results = render_review_filters(
        selections["dataset_path"], selections["language_code"]
    )
    if has_results:
        render_review_clip_section(selections)


if __name__ == "__main__":
    main()
