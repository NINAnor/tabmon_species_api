"""
TABMON Listening Lab - Citizen app.

Open access: pick a country, site and species, listen to random BirdNET
detections and confirm or reject them.
"""

import streamlit as st

from citizen.components import (
    render_all_validated_placeholder,
    render_clip_section,
    render_empty_validation_placeholder,
    render_help_section,
    render_page_header,
)
from citizen.selection import get_user_selections
from citizen.session import (
    get_or_load_clip,
    initialize_session,
    prefetch_next_clip,
)
from citizen.validation import render_validation_form
from core.ui import setup_page_config


def main():
    """Main application entry point for Citizen mode."""
    setup_page_config()
    initialize_session()
    render_page_header()
    render_help_section()

    selections = get_user_selections()
    if not selections:
        return

    st.markdown("---")

    result = get_or_load_clip(selections)

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

    # Runs last so the current clip and form are displayed first.
    if clip_loaded:
        prefetch_next_clip(selections)


if __name__ == "__main__":
    main()
