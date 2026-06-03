"""Review Mode UI Components - clip display, navigation, and cross-validation."""

import os

import pandas as pd
import streamlit as st

from ui.ui_utils import (
    _generate_spectrogram_image,
    render_audio_player,
    render_clip_metadata,
)
from handlers.validation_handlers import _get_all_species_list
from utils import extract_clip, get_species_display_names, save_pro_validation_response


def render_review_clip_section(selections):
    """Render the review mode main content area.

    Layout:
    - Full width: Navigation + Audio player + spectrogram (shared context)
    - Side-by-side below: Original annotation (left) | Your cross-validation (right)

    This lets the reviewer listen once, then directly compare their assessment
    against the original expert's annotation.

    Args:
        selections: dict with dataset_path, user_id, language_code
    """
    results_df = st.session_state.review_results
    current_index = st.session_state.review_current_index

    if results_df is None or results_df.empty:
        st.info("🔍 Apply filters in the sidebar to browse validated clips.")
        return

    # Get current record
    record = results_df.iloc[current_index]

    # Navigation at top
    _render_navigation(len(results_df), current_index)

    # Three-column layout: Audio+Spectrogram | Original Annotation | Your Annotation
    col_spec, col_original, col_yours = st.columns([1, 1, 1])

    with col_spec:
        _render_audio_and_spectrogram(record)

    with col_original:
        _render_original_validation(record, selections)

    with col_yours:
        _render_cross_validation_form(record, selections)


def _render_audio_and_spectrogram(record):
    """Render audio player and spectrogram together in the first column."""
    filename = record.get("filename", "")
    start_time = record.get("start_time", 0)

    if not filename:
        st.warning("No audio file path available for this clip.")
        return

    filepath = filename.replace("bugg_RpiID", "bugg_RPiID")
    full_path = f"s3://{os.getenv('S3_BUCKET')}/{filepath}"

    with st.container(border=True):
        st.markdown("### 🎵 Audio & Spectrogram")

        clip_info = {
            "filename": filename,
            "deployment_id": record.get("deployment_id", ""),
            "start_time": start_time,
        }
        render_clip_metadata(clip_info)

        clip = extract_clip(full_path, start_time)
        render_audio_player(clip)

        # Render spectrogram inline (no expander wrapper)
        img_bytes = _generate_spectrogram_image(full_path, start_time)
        if img_bytes:
            st.image(img_bytes, use_container_width=True)
            st.caption(
                "🔴 Red lines mark the 3-second BirdNET detection."
            )
        else:
            st.warning("Could not generate spectrogram")


def _render_original_validation(record, selections):
    """Render the original validation as a read-only panel (left column)."""
    with st.container(border=True):
        st.markdown("### 📋 Original Annotation")

        # Validator info
        user_confidence = record.get("user_confidence", "")
        confidence_emoji = {
            "Low": "🔴",
            "Moderate": "🟡",
            "High": "🟢",
        }.get(user_confidence, "⚪")

        st.markdown(f"**👤** `{record.get('userID', 'Unknown')}` · "
                    f"**{confidence_emoji}** {user_confidence}")

        timestamp = record.get("timestamp", "")
        if timestamp:
            st.caption(f"🕐 {timestamp}")

        st.markdown("---")

        # Species checklist — show what the original expert selected
        language_code = selections.get("language_code", "Scientific_Name")
        species_str = str(record.get("birdnet_species_detected", ""))
        confidence_str = str(record.get("birdnet_confidences", ""))
        species_list = [s.strip() for s in species_str.split("|") if s.strip()]
        confidence_list = [c.strip() for c in confidence_str.split("|") if c.strip()]

        identified_str = str(record.get("identified_species", ""))
        identified_set = {
            s.strip() for s in identified_str.split("|") if s.strip() and s != "nan"
        }

        if species_list:
            display_map = get_species_display_names(species_list, language_code)
            scientific_to_display = {sci: disp for disp, sci in display_map.items()}

            # Sort by confidence descending (same order as cross-validation form)
            species_data = []
            for idx, species in enumerate(species_list):
                try:
                    conf = (
                        float(confidence_list[idx])
                        if idx < len(confidence_list)
                        else 0.0
                    )
                except (ValueError, TypeError):
                    conf = 0.0
                species_data.append((species, conf))
            species_data.sort(key=lambda x: x[1], reverse=True)

            st.markdown("**Species (BirdNET → Expert):**")
            for species, conf_val in species_data:
                display_name = scientific_to_display.get(species, species)
                was_confirmed = species in identified_set
                icon = "✅" if was_confirmed else "⬜"
                st.markdown(f"{icon} {display_name} (conf: {conf_val:.2f})")

            if "NONE_DETECTED" in identified_set:
                st.markdown("❌ **None of the above confirmed**")
        else:
            st.markdown("*No BirdNET detections*")

        # Additional species identified (not in BirdNET list)
        extra_species = identified_set - set(species_list) - {"NONE_DETECTED", "nan"}
        if extra_species:
            st.markdown("---")
            st.markdown("**➕ Additional species identified:**")
            if language_code != "Scientific_Name":
                extra_display = get_species_display_names(
                    list(extra_species), language_code
                )
                extra_to_display = {sci: disp for disp, sci in extra_display.items()}
            else:
                extra_to_display = {sp: sp for sp in extra_species}
            for species in extra_species:
                st.markdown(f"- {extra_to_display.get(species, species)}")

        # Vocalization types
        vocalization_str = str(record.get("vocalization_types", ""))
        if vocalization_str and vocalization_str != "nan" and vocalization_str.strip():
            st.markdown(f"**🎵** {vocalization_str}")

        # Background sounds and comments
        st.markdown("---")
        user_notes = str(record.get("user_notes", ""))
        if user_notes and user_notes != "nan" and user_notes.strip():
            st.markdown(f"**🔊 Background:** {user_notes}")

        user_comments = str(record.get("user_comments", ""))
        if user_comments and user_comments != "nan" and user_comments.strip():
            st.info(f"💬 {user_comments}")

        # Cross-validation indicator
        is_cross = record.get("is_cross_validation", False)
        if is_cross:
            st.warning("🔄 **This record is itself a cross-validation** — it was not produced by the primary annotator.")


def _render_navigation(total_count, current_index):
    """Render compact navigation bar."""
    col_prev, col_info, col_next = st.columns([1, 3, 1])

    with col_prev:
        if st.button(
            "← Prev",
            disabled=(current_index == 0),
            use_container_width=True,
        ):
            st.session_state.review_current_index = current_index - 1
            st.session_state.review_form_key += 1
            st.rerun()

    with col_info:
        st.markdown(
            f"<div style='text-align: center; padding-top: 0.5rem;'>"
            f"<strong>📍 Clip {current_index + 1} of {total_count}</strong>"
            f"</div>",
            unsafe_allow_html=True,
        )

    with col_next:
        if st.button(
            "Next →",
            disabled=(current_index >= total_count - 1),
            use_container_width=True,
        ):
            st.session_state.review_current_index = current_index + 1
            st.session_state.review_form_key += 1
            st.rerun()


def _render_cross_validation_form(record, selections):
    """Render the cross-validation form (right column).

    Same species checklist order as the original annotation panel
    so the reviewer can directly compare line-by-line.
    """
    with st.container(border=True):
        st.markdown("### 🔄 Your Annotation")

        _render_cross_validated_warning(record, selections)

        fk = st.session_state.get("review_form_key", 0)

        with st.form(f"cross_validation_form_{fk}"):
            # Species from BirdNET detections
            species_str = str(record.get("birdnet_species_detected", ""))
            confidence_str = str(record.get("birdnet_confidences", ""))
            species_list = [s.strip() for s in species_str.split("|") if s.strip()]
            confidence_list = [
                c.strip() for c in confidence_str.split("|") if c.strip()
            ]

            language_code = selections.get("language_code", "Scientific_Name")

            scientific_to_display = {}
            if species_list:
                display_map = get_species_display_names(species_list, language_code)
                scientific_to_display = {
                    sci: disp for disp, sci in display_map.items()
                }

                # Sort by confidence descending (matches original panel order)
                species_data = []
                for idx, species in enumerate(species_list):
                    try:
                        conf = (
                            float(confidence_list[idx])
                            if idx < len(confidence_list)
                            else 0.0
                        )
                    except (ValueError, TypeError):
                        conf = 0.0
                    species_data.append((species, conf))
                species_data.sort(key=lambda x: x[1], reverse=True)

                selected_species = []
                vocalization_types = {}
                for idx, (species, conf_val) in enumerate(species_data):
                    display_name = scientific_to_display.get(species, species)
                    col_check, col_vocal = st.columns([3, 2])
                    with col_check:
                        checked = st.checkbox(
                            f"{display_name} ({conf_val:.2f})",
                            key=f"review_species_{idx}_{fk}",
                        )
                    with col_vocal:
                        vocal_type = st.selectbox(
                            "Type",
                            options=["", "Call", "Song"],
                            key=f"review_vocal_{idx}_{fk}",
                            label_visibility="collapsed",
                        )
                    if checked:
                        selected_species.append(species)
                        if vocal_type:
                            vocalization_types[species] = vocal_type
            else:
                selected_species = []
                vocalization_types = {}

            none_of_above = st.checkbox(
                "❌ None of the above",
                key=f"review_none_{fk}",
            )

            if none_of_above:
                selected_species = ["NONE_DETECTED"]

            # Additional species not detected by BirdNET
            already_shown = set(scientific_to_display.values())
            all_species_options = _get_all_species_list(language_code)
            extra_options = [s for s in all_species_options if s not in already_shown]
            extra_species_raw = st.multiselect(
                "➕ Add species not in BirdNET list",
                options=extra_options,
                key=f"review_extra_species_{fk}",
                placeholder="Start typing to search...",
                help="Add species you hear that BirdNET did not detect",
            )

            st.markdown("---")

            # Confidence rating
            user_confidence = st.radio(
                "**Confidence:**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                key=f"review_confidence_{fk}",
            )

            # Comments
            user_comments = st.text_area(
                "💬 Comments:",
                placeholder="Observations...",
                height=70,
                key=f"review_comments_{fk}",
            )

            submitted = st.form_submit_button(
                "✅ Submit Cross-Validation",
                type="primary",
                use_container_width=True,
            )

            if submitted:
                _handle_cross_validation_submission(
                    record,
                    selections,
                    selected_species,
                    vocalization_types,
                    user_confidence,
                    user_comments,
                    extra_species_raw,
                )


def _render_cross_validated_warning(record, selections):
    """Show a warning if the current user has already cross-validated this clip."""
    from database.review_queries import load_dataset_validations

    dataset_path = selections.get("dataset_path", "")
    user_id = str(selections.get("user_id", ""))
    filename = str(record.get("filename", ""))
    start_time = record.get("start_time", 0)

    all_validations = load_dataset_validations(dataset_path)
    if all_validations.empty or "is_cross_validation" not in all_validations.columns:
        return

    prior = all_validations[
        (all_validations["is_cross_validation"].fillna(False).astype(bool))
        & (all_validations["userID"].astype(str) == user_id)
        & (all_validations["filename"].astype(str) == filename)
        & (all_validations["start_time"].astype(float) == float(start_time))
    ]

    if not prior.empty:
        ts = prior.iloc[0].get("timestamp", "")
        st.warning(
            f"⚠️ You already cross-validated this clip on **{ts}**. "
            "Submitting again will add a duplicate entry."
        )


def _handle_cross_validation_submission(
    record,
    selections,
    selected_species,
    vocalization_types,
    user_confidence,
    user_comments,
    extra_species_raw=None,
):
    """Handle cross-validation form submission."""
    if not user_confidence:
        st.error("Please rate your confidence before submitting.")
        return

    # Parse extra species and merge (format: "Common Name (Scientific Name)" or plain scientific)
    if extra_species_raw and "NONE_DETECTED" not in selected_species:
        additional_species = [
            s.split(" (")[-1].rstrip(")")
            if " (" in s and s.endswith(")")
            else s
            for s in extra_species_raw
        ]
        selected_species = selected_species + additional_species

    from database.queries import get_device_site_map

    deployment_id = record.get("deployment_id", "")
    device_site_map = get_device_site_map()
    site_info = device_site_map.get(deployment_id)
    if not site_info:
        device_id = (
            deployment_id.rsplit("_", 1)[-1] if "_" in deployment_id else deployment_id
        )
        site_info = device_site_map.get(device_id, {})

    # Parse BirdNET species/confidences from the record
    birdnet_species = [
        s.strip()
        for s in str(record.get("birdnet_species_detected", "")).split("|")
        if s.strip()
    ]
    birdnet_confidences = [
        c.strip()
        for c in str(record.get("birdnet_confidences", "")).split("|")
        if c.strip()
    ]

    validation_data = {
        "filename": record.get("filename", ""),
        "userID": selections["user_id"],
        "deployment_id": deployment_id,
        "country": site_info.get("country", "") if isinstance(site_info, dict) else "",
        "site_name": site_info.get("site", "") if isinstance(site_info, dict) else "",
        "cluster": site_info.get("cluster", "") if isinstance(site_info, dict) else "",
        "birdnet_species_detected": birdnet_species,
        "birdnet_confidences": birdnet_confidences,
        "start_time": record.get("start_time", 0),
        "identified_species": selected_species,
        "species_count": len(selected_species),
        "vocalization_types": vocalization_types,
        "user_confidence": user_confidence,
        "user_notes": [],
        "user_comments": user_comments or "",
        "timestamp": pd.Timestamp.now(),
        "is_cross_validation": True,
    }

    success = save_pro_validation_response(validation_data)

    if success:
        from database.review_queries import load_dataset_validations

        load_dataset_validations.clear()
        # Auto-advance to the next clip
        results_df = st.session_state.review_results
        current_index = st.session_state.review_current_index
        if results_df is not None and current_index + 1 < len(results_df):
            st.session_state.review_current_index = current_index + 1
        st.session_state.review_form_key += 1
        st.toast("✅ Cross-validation saved!")
        st.rerun()
