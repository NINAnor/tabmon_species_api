"""Citizen Mode Validation form: Yes/No/Unsure on the selected species."""

import pandas as pd
import streamlit as st

from citizen.queries import get_validation_progress, save_validation
from citizen.session import load_next_clip
from core.shortcuts import add_form_shortcuts
from core.species import extract_scientific, get_all_species_list


def _render_progress(selections):
    validated_count, total_clips = get_validation_progress(selections)
    remaining_clips = total_clips - validated_count

    if total_clips > 0:
        st.progress(validated_count / total_clips)
        st.caption(
            f"✅ {validated_count}/{total_clips} clips validated "
            f"({remaining_clips} remaining)"
        )
    if remaining_clips <= 0:
        st.success("🎉 All clips validated for these parameters!")

    session_count = st.session_state.session_validation_count
    if session_count > 0:
        st.caption(
            f"🏆 You've validated **{session_count}** clip"
            f"{'s' if session_count != 1 else ''} this session!"
        )


def render_validation_form(result, selections):
    """Render the validation form and handle submission."""
    with st.container(border=True):
        st.markdown("### 🎯 Validation")
        _render_progress(selections)

        add_form_shortcuts(
            citizen_answer_=[
                {"key": "y", "option": "Yes"},
                {"key": "n", "option": "No"},
                {"key": "u", "option": "Unsure"},
            ],
            citizen_confidence_=[
                {"key": "q", "option": "Low"},
                {"key": "w", "option": "Moderate"},
                {"key": "e", "option": "High"},
            ],
            citizen_submit_={"key": " ", "action": "click"},
        )

        form_key = st.session_state.validation_form_key
        with st.form(f"validation_form_{form_key}"):
            st.markdown(f"#### Is this detection a {selections['species_display']}?")

            wiki_url = "https://en.wikipedia.org/wiki/" + selections["species"].replace(
                " ", "_"
            )
            xeno_canto_url = "https://xeno-canto.org/explore?query=" + selections[
                "species"
            ].replace(" ", "+")
            st.markdown(
                f"ℹ️ [Wikipedia]({wiki_url}) · "
                f"🔊 [Listen on xeno-canto]({xeno_canto_url})"
            )

            validation_response = st.radio(
                "**Your answer:**",
                options=["Yes", "No", "Unsure"],
                index=None,
                horizontal=True,
                key=f"citizen_answer_{form_key}",
                help="Help us validate the accuracy of our species detection models!",
            )

            user_validation = st.multiselect(
                "**If no, what species did you hear instead?**",
                options=get_all_species_list(selections["language_code"]),
                default=[],
                help="Search and select the species you actually heard "
                "in this audio clip",
                placeholder="Start typing to search...",
            )

            user_confidence = st.radio(
                "**How confident are you in your answer?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                key=f"citizen_confidence_{form_key}",
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
                "✅ Submit Validation",
                type="primary",
                use_container_width=True,
                key=f"citizen_submit_{form_key}",
            )

        if not submitted:
            return

        if not (validation_response and user_confidence):
            st.error("Please answer both questions before submitting.")
            return

        saved = save_validation(
            {
                "filename": result["filename"],
                "country": selections["country"],
                "site": selections["site_name"],
                "device_id": selections["device"],
                "species": selections["species"],
                "start_time": result["start_time"],
                "confidence": result["confidence"],
                "validation_response": validation_response,
                "user_validation": [extract_scientific(s) for s in user_validation],
                "user_confidence": user_confidence,
                "user_comments": user_comments,
                "annotator_name": annotator_name,
                "timestamp": pd.Timestamp.now(),
            }
        )
        if not saved:
            return

        st.session_state.session_validation_count += 1
        load_next_clip()
        st.toast("✅ Validation saved! Loading next clip...")
        st.rerun()
