"""Review Mode UI Components - clip display, navigation, and cross-validation."""

import os

import pandas as pd
import streamlit as st

from handlers.validation_handlers import (
    NO_BIRD_AUDIBLE,
    _build_fp_columns,
    _collect_fp_widget_values,
    _extract_scientific,
    _get_all_species_list,
    _parse_validation_record_for_prefill,
    render_fp_characterization_fragment,
)
from ui.ui_shortcuts import add_form_shortcuts
from ui.ui_utils import (
    _generate_spectrogram_image,
    render_audio_player,
    render_clip_metadata,
)
from utils import (
    extract_clip,
    get_species_display_names,
    parse_list_cell,
    save_pro_validation_response,
)


def _parse_birdnet_lists(record):
    """Parse BirdNET species/confidences from a review-mode record.

    Returns (species_list, confidence_list, species_data):
    - species_list: stripped scientific names (pipe-split)
    - confidence_list: stripped confidence strings (pipe-split, unvalidated)
    - species_data: list of (species, float_confidence) sorted by
      confidence descending, 0.0 fallback for missing/invalid values
      (including "nan" strings)
    """
    species_list = [
        s.strip()
        for s in str(record.get("birdnet_species_detected", "")).split("|")
        if s.strip()
    ]
    confidence_list = [
        c.strip()
        for c in str(record.get("birdnet_confidences", "")).split("|")
        if c.strip()
    ]
    species_data = []
    for idx, species in enumerate(species_list):
        conf = 0.0
        if idx < len(confidence_list):
            try:
                parsed = float(confidence_list[idx])
                if parsed == parsed:  # exclude "nan" strings
                    conf = parsed
            except (ValueError, TypeError):
                pass
        species_data.append((species, conf))
    species_data.sort(key=lambda x: x[1], reverse=True)
    return species_list, confidence_list, species_data


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

        context_seconds = st.slider(
            "Context around detection (s)",
            min_value=1,
            max_value=5,
            value=1,
            step=1,
            key="review_context_seconds",
            help="Seconds of audio context before and after the 3-second BirdNET detection window.",
        )

        clip = extract_clip(full_path, start_time, context_seconds=context_seconds)
        render_audio_player(clip)

        # Render spectrogram inline (no expander wrapper)
        img_bytes = _generate_spectrogram_image(full_path, start_time, context_seconds)
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
        species_list, _conf_strings, species_data = _parse_birdnet_lists(record)

        identified_str = str(record.get("identified_species", ""))
        identified_set = {
            s.strip() for s in identified_str.split("|") if s.strip() and s != "nan"
        }

        if species_list:
            display_map = get_species_display_names(species_list, language_code)
            scientific_to_display = {sci: disp for disp, sci in display_map.items()}

            st.markdown("**Species (BirdNET → Expert):**")
            for species, conf_val in species_data:
                display_name = scientific_to_display.get(species, species)
                was_confirmed = species in identified_set
                icon = "✅" if was_confirmed else "⬜"
                st.markdown(f"{icon} {display_name} (conf: {conf_val:.2f})")

            if "NONE_DETECTED" in identified_set:
                st.markdown("❌ **None of the listed species are present**")
            if NO_BIRD_AUDIBLE in identified_set:
                st.markdown("🕊️ **No bird species audible**")
            if "UNIDENTIFIED_BIRD_PRESENT" in identified_set:
                st.markdown(
                    "🤷 **A bird was present but species could not be determined**"
                )
        else:
            st.markdown("*No BirdNET detections*")

        # Additional species identified (not in BirdNET list)
        extra_species = identified_set - set(species_list) - {
            "NONE_DETECTED",
            "UNIDENTIFIED_BIRD_PRESENT",
            NO_BIRD_AUDIBLE,
            "nan",
        }
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

        # False Positive characterization (if this clip was characterized
        # as a FP during the primary validation pass)
        fp_categories = parse_list_cell(record.get("fp_source_category"))
        if fp_categories:
            st.markdown("---")
            st.markdown("**🔬 False Positive Characterization:**")

            fp_details = parse_list_cell(record.get("fp_source_detail"))
            source_parts = []
            for idx, fp_category in enumerate(fp_categories):
                fp_detail = fp_details[idx] if idx < len(fp_details) else ""
                if fp_category == "Bird" and fp_detail:
                    detail_display_map = get_species_display_names(
                        [fp_detail], language_code
                    )
                    detail_to_display = {
                        sci: disp for disp, sci in detail_display_map.items()
                    }
                    source_parts.append(
                        f"Specific bird species — "
                        f"{detail_to_display.get(fp_detail, fp_detail)}"
                    )
                elif fp_category == "Other bird" and fp_detail:
                    source_parts.append(f"Other bird (unidentified) — {fp_detail}")
                elif fp_detail:
                    source_parts.append(f"{fp_category} — {fp_detail}")
                else:
                    source_parts.append(fp_category)
            st.markdown("- **Source/confuser:** " + "; ".join(source_parts))

            fp_target_str = str(record.get("fp_target_species", "")).strip()
            if fp_target_str and fp_target_str != "nan":
                fp_targets = [s.strip() for s in fp_target_str.split("|") if s.strip()]
                target_display_map = get_species_display_names(
                    fp_targets, language_code
                )
                target_to_display = {
                    sci: disp for disp, sci in target_display_map.items()
                }
                targets_display = ", ".join(
                    target_to_display.get(sp, sp) for sp in fp_targets
                )
                st.markdown(f"- **Rejected species (confused for):** {targets_display}")

            fp_vocal = str(record.get("fp_confuser_vocalization", "")).strip()
            if fp_vocal and fp_vocal != "nan":
                st.markdown(f"- **Confuser vocalization type:** {fp_vocal}")

            fp_mechanism_str = str(record.get("fp_likely_mechanism", "")).strip()
            if fp_mechanism_str and fp_mechanism_str != "nan":
                st.markdown(
                    f"- **Likely confusion mechanism(s):** "
                    f"{fp_mechanism_str.replace('|', ', ')}"
                )

            fp_certainty = str(record.get("fp_certainty", "")).strip()
            if fp_certainty and fp_certainty != "nan":
                st.markdown(f"- **Certainty:** {fp_certainty}")

            fp_comment = str(record.get("fp_comment", "")).strip()
            if fp_comment and fp_comment != "nan":
                st.info(f"💬 FP comment: {fp_comment}")

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
            shortcut="a",
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
            shortcut="d",
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
        language_code = selections.get("language_code", "Scientific_Name")

        # Automatically determine update vs. cross-validation from the
        # original annotator's userID vs. the currently logged-in reviewer —
        # no manual toggle needed, so submitting only takes a single click.
        original_user_id = str(record.get("userID", "")).strip()
        current_user_id = str(selections.get("user_id", "")).strip()
        is_update_mode = bool(original_user_id) and (
            original_user_id == current_user_id
        )

        # FP characterization — rendered ABOVE the form inside a fragment so
        # the assessment checkbox instantly opens the FP fields (no page
        # rerun beyond the fragment itself). Pre-filled from the record when
        # the reviewer is the original annotator (update mode). Values are
        # collected from session state at submit time.
        review_all_species = _get_all_species_list(language_code)
        last_fp = {}
        parsed_own = {}
        if is_update_mode:
            parsed_own = _parse_validation_record_for_prefill(
                record, language_code, review_all_species
            )
            last_fp = parsed_own.get("fp_form_data") or {}
        render_fp_characterization_fragment("xfp", fk, review_all_species, last_fp)

        with st.form(f"cross_validation_form_{fk}"):
            if is_update_mode:
                st.info(
                    "✏️ You are the original validator for this clip — "
                    "submitting will be recorded as an **update**."
                )
            else:
                st.caption(
                    "🔄 Submitting will be recorded as a "
                    "**cross-validation** of another annotator's work."
                )

            # Pre-parse original vocalization types for pre-filling when in update mode
            original_voc_str = str(record.get("vocalization_types", ""))
            original_voc_map = {}
            if original_voc_str and original_voc_str != "nan":
                for pair in original_voc_str.split("|"):
                    if ":" in pair:
                        sp, vtype = pair.split(":", 1)
                        original_voc_map[sp.strip()] = vtype.strip()

            # Species from BirdNET detections
            species_list, _conf_strings, species_data = _parse_birdnet_lists(record)

            scientific_to_display = {}
            if species_list:
                display_map = get_species_display_names(species_list, language_code)
                scientific_to_display = {
                    sci: disp for disp, sci in display_map.items()
                }

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
                        prefilled_voc = original_voc_map.get(species, "")
                        voc_options = ["", "Call", "Song"]
                        voc_default_idx = voc_options.index(prefilled_voc) if is_update_mode and prefilled_voc in voc_options else 0
                        vocal_type = st.selectbox(
                            "Type",
                            options=voc_options,
                            index=voc_default_idx,
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
                "❌ None of the listed species are present",
                value=parsed_own.get("none_of_above", False),
                key=f"review_none_{fk}",
                help=(
                    "Check this if none of the BirdNET-detected species are "
                    "actually present"
                ),
            )

            if none_of_above:
                selected_species.append("NONE_DETECTED")

            no_bird_audible = st.checkbox(
                "🕊️ No bird species audible",
                value=parsed_own.get("no_bird_audible", False),
                key=f"review_no_bird_{fk}",
                help=(
                    "Check this if no bird of any kind is audible in this "
                    "clip at all"
                ),
            )

            if no_bird_audible:
                selected_species.append(NO_BIRD_AUDIBLE)

            unidentified_bird = st.checkbox(
                "🤷 A bird is present but I can't determine the species",
                value=parsed_own.get("unidentified_bird", False),
                key=f"review_unidentified_{fk}",
            )
            if unidentified_bird:
                selected_species.append("UNIDENTIFIED_BIRD_PRESENT")

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

            user_confidence = st.radio(
                "**Confidence:**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                key=f"review_confidence_{fk}",
            )
            shortcut_bindings = {
                f"review_none_{fk}": "2",
                f"review_no_bird_{fk}": "3",
                f"review_unidentified_{fk}": "4",
                f"review_confidence_{fk}": [
                    {"shortcut": "q", "option": "Low"},
                    {"shortcut": "w", "option": "Moderate"},
                    {"shortcut": "e", "option": "High"},
                ],
            }
            if species_list:
                shortcut_bindings[f"review_species_0_{fk}"] = "1"
            add_form_shortcuts(**shortcut_bindings)

            st.markdown("---")

            # Comments — pre-filled from original when in update mode
            original_comments = str(record.get("user_comments", ""))
            original_comments = "" if original_comments == "nan" else original_comments
            user_comments = st.text_area(
                "💬 Comments:",
                value=original_comments if is_update_mode else "",
                placeholder="Observations...",
                height=70,
                key=f"review_comments_{fk}",
            )

            submit_label = "✅ Submit Update" if is_update_mode else "✅ Submit Cross-Validation"
            submitted = st.form_submit_button(
                submit_label,
                type="primary",
                use_container_width=True,
                shortcut="Enter",
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
                    is_update=is_update_mode,
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
    is_update=False,
):
    """Handle cross-validation or update form submission."""
    # Collect the FP fragment's widget values once (the block lives outside
    # the form).
    fp_values = _collect_fp_widget_values(
        "xfp", st.session_state.get("review_form_key", 0)
    )

    # Confidence gating (user feedback): block only if NEITHER the
    # annotation confidence NOR the FP certainty is selected.
    if not user_confidence and not fp_values.get("fp_certainty"):
        st.error(
            "Please rate your confidence in the annotation above, or the "
            "certainty of the False-Positive assessment."
        )
        return

    sentinels = {
        "NONE_DETECTED",
        NO_BIRD_AUDIBLE,
        "UNIDENTIFIED_BIRD_PRESENT",
    }
    has_confirmed_species = any(
        species not in sentinels for species in selected_species
    )
    has_none = "NONE_DETECTED" in selected_species
    has_silence = NO_BIRD_AUDIBLE in selected_species
    has_unidentified = "UNIDENTIFIED_BIRD_PRESENT" in selected_species
    has_other_species = bool(extra_species_raw)

    if has_none and has_confirmed_species:
        st.error(
            "Contradictory annotation: you confirmed species and selected "
            "'None of the listed species are present'."
        )
        return
    if has_silence and any(
        [has_confirmed_species, has_unidentified, has_other_species]
    ):
        st.error(
            "Contradictory annotation: 'No bird species audible' conflicts "
            "with the species marked above."
        )
        return

    # Parse extra species and merge (format: "Common Name (Scientific Name)" or plain scientific)
    if extra_species_raw and "NONE_DETECTED" not in selected_species:
        additional_species = [_extract_scientific(s) for s in extra_species_raw]
        selected_species = selected_species + additional_species
    from database.queries import get_site_info_for_deployment

    deployment_id = record.get("deployment_id", "")
    site_info = get_site_info_for_deployment(deployment_id)

    # Parse BirdNET species/confidences from the record
    birdnet_species, birdnet_confidences, _ = _parse_birdnet_lists(record)

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
        "is_cross_validation": not is_update,
        "is_update": is_update,
        "is_fp_assessment": False,
    }

    # FP characterization: apply the same consistency rules as the expert
    # validation form. A FP assessment is only meaningful if at least one
    # BirdNET detection was NOT confirmed.
    rejected_species = []
    for idx, species in enumerate(birdnet_species):
        if species in selected_species:
            continue
        try:
            conf = (
                float(birdnet_confidences[idx])
                if idx < len(birdnet_confidences)
                else 0.0
            )
        except (ValueError, TypeError):
            conf = 0.0
        rejected_species.append((species, conf))
    error, fp_columns = _build_fp_columns(
        fp_values, rejected_species, bool(birdnet_species)
    )
    if error:
        st.error(error)
        return
    validation_data.update(fp_columns)

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
