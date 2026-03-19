"""
Validation Handlers for the TABMON Listening Lab dashboard.

This module handles the validation form rendering, user input collection,
and submission of validation responses.
"""

import pandas as pd
import streamlit as st

from config import LANGUAGE_MAPPING
from queries import get_all_clips_for_species, get_remaining_clips_count
from utils import load_species_translations, save_validation_response


@st.cache_data
def _get_all_species_list(language_code="Scientific_Name"):
    """Get species names for autocomplete in selected language."""
    translations_df = load_species_translations()

    if (
        language_code == "Scientific_Name"
        or language_code not in translations_df.columns
    ):
        return sorted(translations_df["Scientific_Name"].dropna().tolist())

    species_list = [
        f"{row[language_code]} ({row['Scientific_Name']})"
        if pd.notna(row.get(language_code))
        else row["Scientific_Name"]
        for _, row in translations_df.iterrows()
    ]
    return sorted(species_list)


def render_validation_form(result, selections):
    """
    Render the validation form and handle submission.

    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Validation")

        # Show remaining clips count with progress bar
        remaining_clips = get_remaining_clips_count(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )
        _, total_clips = get_all_clips_for_species(
            selections["country"],
            selections["device"],
            selections["species"],
            selections["confidence_threshold"],
        )
        validated_count = max(0, total_clips - remaining_clips)

        if total_clips > 0:
            progress = validated_count / total_clips
            st.progress(progress)
            st.caption(
                f"✅ {validated_count}/{total_clips} clips validated "
                f"({remaining_clips} remaining)"
            )
        if remaining_clips <= 0:
            st.success("🎉 All clips validated for these parameters!")

        # Session counter
        session_count = st.session_state.get("session_validation_count", 0)
        if session_count > 0:
            st.caption(
                f"🏆 You've validated **{session_count}** clip"
                f"{'s' if session_count != 1 else ''} this session!"
            )

        with st.form("validation_form"):
            st.markdown(f"#### Is this detection a {selections['species_display']}?")

            # Reference links for the species
            species_wiki_name = selections["species"].replace(" ", "_")
            wiki_url = f"https://en.wikipedia.org/wiki/{species_wiki_name}"
            xc_query = selections["species"].replace(" ", "+")
            xc_url = f"https://xeno-canto.org/explore?query={xc_query}"
            st.markdown(
                f"ℹ️ [Wikipedia]({wiki_url}) · 🔊 [Listen on xeno-canto]({xc_url})",
            )

            validation_response = st.radio(
                "**Your answer:**",
                options=["Yes", "No", "Unsure"],
                index=None,
                horizontal=True,
                help="Help us validate the accuracy of our species detection models!",
            )

            # Get species list in the user's selected language
            selected_language = selections.get("language", "Scientific Names")
            if selected_language == "Scientific Names":
                language_code = "Scientific_Name"
            else:
                language_code = LANGUAGE_MAPPING.get(selected_language, "Scientific_Name")
            all_species = _get_all_species_list(language_code)

            user_validation = st.multiselect(
                "**If no, what species did you hear instead?**",
                options=all_species,
                default=[],
                help="Search and select the species you actually heard in this audio clip",
                placeholder="Start typing to search...",
            )

            user_confidence = st.radio(
                "**How confident are you in your answer?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                help="Rate your confidence in the validation above",
            )

            user_comments = st.text_area(
                "**💬 Comments (optional)**",
                placeholder=(
                    "E.g., 'Faint call in background', 'Multiple species present'..."
                ),
                height=80,
                help="Any additional observations about the clip",
            )

            annotator_name = st.text_input(
                "**👤 Annotator Name (optional)**",
                placeholder="Enter your name",
                help="Your name will be recorded with the validation",
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation", type="primary", use_container_width=True
            )

        if submitted:
            _handle_validation_submission(
                result,
                selections,
                validation_response,
                user_validation,
                user_confidence,
                user_comments,
                annotator_name,
            )


def _handle_validation_submission(
    result,
    selections,
    validation_response,
    user_validation,
    user_confidence,
    user_comments,
    annotator_name,
):
    """Handle validation form submission."""
    if validation_response and user_confidence:
        # Extract scientific names from multiselect
        # Format: "Common Name (Scientific Name)"
        detected_species = [
            species_str.split(" (")[-1].rstrip(")")
            if " (" in species_str and species_str.endswith(")")
            else species_str
            for species_str in user_validation
        ]

        validation_data = {
            "filename": result["filename"],
            "country": selections["country"],
            "site": selections["site_name"],
            "device_id": selections["device"],
            "species": selections["species"],
            "start_time": result["start_time"],
            "confidence": result["confidence"],
            "validation_response": validation_response,
            "user_validation": detected_species,
            "user_confidence": user_confidence,
            "user_comments": user_comments,
            "annotator_name": annotator_name,
            "timestamp": pd.Timestamp.now(),
        }

        save_validation_response(validation_data)

        # Track session progress
        st.session_state.session_validation_count = (
            st.session_state.get("session_validation_count", 0) + 1
        )

        # Clear current clip so the next rerun loads a fresh one
        st.session_state.current_clip = None

        st.toast("✅ Validation saved! Loading next clip...")
        st.rerun()
    else:
        st.error("Please answer both questions before submitting.")
