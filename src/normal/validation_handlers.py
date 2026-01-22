"""
Validation Handlers for the TABMON Listening Lab dashboard.

This module handles the validation form rendering, user input collection,
and submission of validation responses.
"""

import pandas as pd
import streamlit as st

from shared.queries import get_remaining_clips_count
from shared.utils import save_validation_response


def render_validation_form(result, selections):
    """
    Render the validation form and handle submission.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Validation")

        # Show remaining clips count
        remaining_clips = get_remaining_clips_count(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )
        if remaining_clips > 0:
            st.info(
                f"📊 Still **{remaining_clips}** clips to annotate for your "
                f"current parameters"
            )
        else:
            st.success("🎉 All clips validated for these parameters!")

        with st.form("validation_form"):
            st.markdown(f"#### Is this detection a {selections['species_display']}?")

            # Add Wikipedia link for the species
            species_wiki_name = selections["species"].replace(" ", "_")
            wiki_url = f"https://en.wikipedia.org/wiki/{species_wiki_name}"
            st.markdown(
                f"ℹ️ Learn more about this species: "
                f"[Wikipedia page for {selections['species']}]({wiki_url})",
                unsafe_allow_html=True,
            )

            validation_response = st.radio(
                "**Your answer:**",
                options=["Yes", "No", "Unsure"],
                index=None,
                horizontal=True,
                help="Help us validate the accuracy of our species detection models!",
            )

            user_validation = st.text_input(
                "**If no, What did you detect instead?**",
                placeholder="e.g., different species, noise, silence, etc.",
                help="Please describe what you actually heard in this audio clip",
            )

            user_confidence = st.radio(
                "**How confident are you in your answer?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                help="Rate your confidence in the validation above",
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation", type="primary", use_container_width=True
            )

        if submitted:
            _handle_validation_submission(
                result, selections, validation_response, user_validation, user_confidence
            )


def _handle_validation_submission(
    result, selections, validation_response, user_validation, user_confidence
):
    """
    Handle validation form submission.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
        validation_response: User's Yes/No/Unsure response
        user_validation: User's text description of what they heard
        user_confidence: User's confidence level
    """
    if validation_response and user_confidence:
        validation_data = {
            "filename": result["filename"],
            "country": selections["country"],
            "site": selections["site_name"],
            "device_id": selections["device"],
            "species": selections["species"],
            "start_time": result["start_time"],
            "confidence": result["confidence"],
            "validation_response": validation_response,
            "user_validation": user_validation,
            "user_confidence": user_confidence,
            "timestamp": pd.Timestamp.now(),
        }

        save_validation_response(validation_data)

        st.success("✅ Thank you for your time and effort!")
        st.rerun()
    else:
        st.error("Please answer both questions before submitting.")
