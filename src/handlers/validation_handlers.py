"""Expert Mode Validation Handlers - checklist of top species."""

import pandas as pd
import streamlit as st
from streamlit_shortcuts import add_shortcuts

from utils import get_species_display_names, load_species_translations


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


def _scientific_to_display_option(scientific_name, language_code, all_species):
    """Find the exact option string (matching a selectbox/multiselect's
    `options=all_species`) for a given pure scientific name.

    Needed because saved records always store pure scientific names, but
    widget options may be "Common Name (Scientific Name)" when a non-English
    display language is active. Returns None if no match is found, so the
    caller can safely fall back to a blank default instead of crashing.
    """
    if not scientific_name:
        return None
    if language_code == "Scientific_Name":
        return scientific_name if scientific_name in all_species else None

    # get_species_display_names returns the bare common name as the dict
    # key (e.g. "Rufous-faced Warbler"), but _get_all_species_list's
    # options are in the combined "Common Name (Scientific Name)" format
    # (falling back to the bare scientific name when no translation
    # exists). Reconstruct the same combined format here so it actually
    # matches an entry in `all_species`.
    display_map = get_species_display_names([scientific_name], language_code)
    common_name = next(iter(display_map.keys()), None)
    if common_name is None:
        return None
    candidate = (
        scientific_name
        if common_name == scientific_name
        else f"{common_name} ({scientific_name})"
    )
    return candidate if candidate in all_species else None


def _parse_validation_record_for_prefill(record, language_code, all_species):
    """Convert a saved validation CSV row back into the same shape used to
    pre-fill the form's widgets (species, vocalization, FP block, etc.).

    Mirrors the parsing already done for Review Mode's cross-validation form,
    but produces the exact dict shape `render_pro_validation_form` expects.
    """

    def _clean(value):
        value = "" if value is None else str(value)
        return "" if value.lower() == "nan" else value

    identified_str = _clean(record.get("identified_species"))
    identified_list = [s.strip() for s in identified_str.split("|") if s.strip()]

    detected_str = _clean(record.get("birdnet_species_detected"))
    detected_list = [s.strip() for s in detected_str.split("|") if s.strip()]

    none_of_above = "NONE_DETECTED" in identified_list
    unidentified_bird = "UNIDENTIFIED_BIRD_PRESENT" in identified_list
    species_only_selected = [s for s in identified_list if s in detected_list]
    other_species_raw = [
        s
        for s in identified_list
        if s not in detected_list
        and s not in ("NONE_DETECTED", "UNIDENTIFIED_BIRD_PRESENT")
    ]
    other_species_list = [
        _scientific_to_display_option(sci, language_code, all_species) or sci
        for sci in other_species_raw
    ]

    vocalization_types = {}
    voc_str = _clean(record.get("vocalization_types"))
    if voc_str:
        for pair in voc_str.split("|"):
            if ":" in pair:
                sp, vtype = pair.split(":", 1)
                vocalization_types[sp.strip()] = vtype.strip()

    user_notes_str = _clean(record.get("user_notes"))
    user_notes = [s.strip() for s in user_notes_str.split("|") if s.strip()]

    # Reconstruct the original "Likely FP source/confuser" dropdown label
    # from the saved fp_source_category / fp_source_detail columns.
    fp_category = _clean(record.get("fp_source_category"))
    fp_detail = _clean(record.get("fp_source_detail"))
    fp_species_raw = ""
    fp_other_bird_text = ""
    if fp_category == "Bird":
        fp_source = "Specific bird species (choose below)"
        fp_species_raw = fp_detail
    elif fp_category == "Other bird":
        fp_source = "Other bird (unidentified — describe below)"
        fp_other_bird_text = fp_detail
    elif fp_category == "Unclear":
        fp_source = "Unclear"
    elif fp_category and fp_detail:
        fp_source = f"{fp_category} — {fp_detail}"
    else:
        fp_source = ""

    fp_species = _scientific_to_display_option(
        fp_species_raw, language_code, all_species
    )

    fp_mechanism_str = _clean(record.get("fp_likely_mechanism"))
    fp_mechanism = [s.strip() for s in fp_mechanism_str.split("|") if s.strip()]

    return {
        "species_only_selected": species_only_selected,
        "vocalization_types": vocalization_types,
        "none_of_above": none_of_above,
        "unidentified_bird": unidentified_bird,
        "other_species_list": other_species_list,
        "user_confidence": _clean(record.get("user_confidence")) or None,
        "user_notes": user_notes,
        "user_comments": _clean(record.get("user_comments")),
        "fp_form_data": {
            "fp_source": fp_source,
            "fp_species": fp_species,
            "fp_other_bird_text": fp_other_bird_text,
            "fp_vocalization": _clean(record.get("fp_confuser_vocalization")),
            "fp_mechanism": fp_mechanism,
            "fp_certainty": _clean(record.get("fp_certainty")) or None,
            "fp_comment": _clean(record.get("fp_comment")),
        },
    }


def render_pro_validation_form(result, selections):
    """
    Render the Expert mode validation form with species checklist.

    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Expert Validation")

        from database.queries import (
            get_remaining_pro_clips_count,
            get_user_validation_records,
        )

        clips = st.session_state.get("expert_full_clip_list") or []
        index = st.session_state.get("expert_clip_index", 0)
        validation_records = get_user_validation_records(
            selections["user_id"], selections["dataset_path"]
        )
        validated_keys = set(validation_records.keys())

        if clips:
            remaining = get_remaining_pro_clips_count(clips, validated_keys)
            st.info(
                f"📍 Clip **{index + 1}** of **{len(clips)}** · "
                f"**{remaining}** still unannotated"
            )

        fk = st.session_state.get("expert_form_key", 0)

        # If this clip already has a submitted record (whether you arrived
        # here via Previous, Next, or a fresh load), pre-fill every widget
        # below with what was submitted, so you only need to change the one
        # thing you got wrong. Resubmitting appends a new is_update=True row.
        record_key = (result.get("filename"), result.get("start_time"))
        existing_record = validation_records.get(record_key)
        language_code = selections.get("language_code", "Scientific_Name")
        all_species_for_prefill = _get_all_species_list(language_code)
        last_fields = (
            _parse_validation_record_for_prefill(
                existing_record, language_code, all_species_for_prefill
            )
            if existing_record is not None
            else {}
        )
        last_fp = last_fields.get("fp_form_data") or {}

        with st.form(f"expert_validation_form_{fk}"):
            st.markdown("#### Species detected by BirdNET in this clip:")
            st.markdown("**Select which species you can actually hear:**")
            st.markdown("---")

            # Get species and confidence arrays (DuckDB returns actual lists)
            species_list = result.get("species_array", []) or []
            confidence_list = result.get("confidence_array", []) or []

            # Sort species by confidence (descending)
            species_data = sorted(
                [
                    (
                        species,
                        float(confidence_list[idx])
                        if idx < len(confidence_list)
                        else 0.0,
                    )
                    for idx, species in enumerate(species_list)
                ],
                key=lambda x: x[1],
                reverse=True,
            )

            # Get translations
            scientific_names = [species for species, _ in species_data]
            display_name_map = get_species_display_names(
                scientific_names, language_code
            )
            scientific_to_display = {
                sci: disp for disp, sci in display_name_map.items()
            }

            # Get all available species for autocomplete (used both for the
            # "Other species not listed above" picker and the clip-level FP
            # confuser search below)
            all_species = all_species_for_prefill

            # Display checklist. Picking a vocalization type alone (without
            # also checking the box) is enough to mark a species present —
            # the checkbox remains available for Ctrl+1/Ctrl+2 shortcuts and
            # for marking presence without committing to Call/Song.
            species_only_selected = []
            vocalization_types = {}
            prefilled_species = last_fields.get("species_only_selected", [])
            prefilled_vocal_types = last_fields.get("vocalization_types", {})
            voc_options = ["", "Call", "Song", "Other/Unknown"]
            for idx, (species, conf_val) in enumerate(species_data):
                display_name = scientific_to_display.get(species, species)
                col_check, col_vocal = st.columns([3, 2])
                with col_check:
                    checked = st.checkbox(
                        f"{display_name} (Birdnet conf: {conf_val:.2f})",
                        value=species in prefilled_species,
                        key=f"species_{idx}_{fk}",
                    )
                with col_vocal:
                    prefilled_voc = prefilled_vocal_types.get(species, "")
                    voc_index = (
                        voc_options.index(prefilled_voc)
                        if prefilled_voc in voc_options
                        else 0
                    )
                    vocal_type = st.selectbox(
                        "Vocalization",
                        options=voc_options,
                        index=voc_index,
                        key=f"vocal_{idx}_{fk}",
                        label_visibility="collapsed",
                    )
                if checked or vocal_type:
                    species_only_selected.append(species)
                    if vocal_type:
                        vocalization_types[species] = vocal_type

            none_of_above = st.checkbox(
                "❌ No bird species audible",
                value=last_fields.get("none_of_above", False),
                key=f"none_of_above_{fk}",
                help="Check this if you cannot hear any bird in this clip at all",
            )

            unidentified_bird = st.checkbox(
                "🤷 A bird is present but I can't determine the species",
                value=last_fields.get("unidentified_bird", False),
                key=f"unidentified_bird_{fk}",
                help=(
                    "Check this if you can hear a bird call/song but cannot "
                    "match it to any specific species"
                ),
            )

            # Keyboard shortcuts: Ctrl+1 / Ctrl+2 toggle the top two species
            # checkboxes, Ctrl+3 toggles "No bird species audible", Ctrl+4
            # toggles "Unidentified bird present". Guarded so we never
            # reference a checkbox key that doesn't exist for clips with
            # fewer than 2 BirdNET detections.
            shortcut_bindings = {}
            if len(species_data) >= 1:
                shortcut_bindings[f"species_0_{fk}"] = "ctrl+1"
            if len(species_data) >= 2:
                shortcut_bindings[f"species_1_{fk}"] = "ctrl+2"
            shortcut_bindings[f"none_of_above_{fk}"] = "ctrl+3"
            shortcut_bindings[f"unidentified_bird_{fk}"] = "ctrl+4"
            add_shortcuts(**shortcut_bindings)

            # Clip-level False Positive characterization — applies whenever
            # one or more BirdNET-detected species were rejected (left
            # unchecked, or covered by "No bird species audible"). One block
            # per clip rather than per species, since the FP source/confuser
            # typically describes the whole clip, not an individual row.
            with st.expander("🔬 Characterize as False Positive (optional)"):
                fp_source_options = [
                    "— Not a false positive / N/A —",
                    "Specific bird species (choose below)",
                    "Other bird (unidentified — describe below)",
                    "Other animal — Amphibian/Frogs",
                    "Other animal — Insect/Cricket",
                    "Other animal — Other Animal",
                    "Environmental — Rain",
                    "Environmental — Wind",
                    "Environmental — Other Environmental Noise",
                    "Anthropogenic — Aircraft",
                    "Anthropogenic — Traffic/Car",
                    "Anthropogenic — Other Anthropogenic",
                    "Unclear",
                ]
                prefilled_fp_source = last_fp.get("fp_source", "")
                fp_source = st.selectbox(
                    "Likely FP source/confuser",
                    options=fp_source_options,
                    index=(
                        fp_source_options.index(prefilled_fp_source)
                        if prefilled_fp_source in fp_source_options
                        else 0
                    ),
                    key=f"fp_source_{fk}",
                )
                prefilled_fp_species = last_fp.get("fp_species")
                fp_species = st.selectbox(
                    "If 'Specific bird species': which species?",
                    options=all_species,
                    index=(
                        all_species.index(prefilled_fp_species)
                        if prefilled_fp_species in all_species
                        else None
                    ),
                    placeholder="Start typing to search...",
                    key=f"fp_species_{fk}",
                )
                fp_other_bird_text = st.text_input(
                    "If 'Other bird': describe "
                    "(e.g. 'lark, sparrow, finch, etc.')",
                    value=last_fp.get("fp_other_bird_text", ""),
                    key=f"fp_other_bird_{fk}",
                )
                fp_vocal_options = ["", "Song", "Call", "Other/Unknown"]
                prefilled_fp_vocal = last_fp.get("fp_vocalization", "")
                fp_vocalization = st.selectbox(
                    "Vocalization type of confuser (optional)",
                    options=fp_vocal_options,
                    index=(
                        fp_vocal_options.index(prefilled_fp_vocal)
                        if prefilled_fp_vocal in fp_vocal_options
                        else 0
                    ),
                    key=f"fp_vocal_{fk}",
                )
                st.markdown("**Likely confusion mechanism:**")
                mechanism_options = [
                    "Acoustic similarity",
                    "Partial vocalisation",
                    "Overlapping signal",
                    "Low SNR / masking",
                    "Other / unclear",
                ]
                prefilled_mechanisms = last_fp.get("fp_mechanism", [])
                fp_mechanism = [
                    mechanism
                    for mechanism in mechanism_options
                    if st.checkbox(
                        mechanism,
                        value=mechanism in prefilled_mechanisms,
                        key=f"fp_mechanism_{mechanism}_{fk}",
                    )
                ]
                fp_certainty_options = ["Low", "Medium", "High"]
                prefilled_fp_certainty = last_fp.get("fp_certainty")
                fp_certainty = st.radio(
                    "Certainty",
                    options=fp_certainty_options,
                    index=(
                        fp_certainty_options.index(prefilled_fp_certainty)
                        if prefilled_fp_certainty in fp_certainty_options
                        else None
                    ),
                    horizontal=True,
                    key=f"fp_certainty_{fk}",
                )
                fp_comment = st.text_input(
                    "Comment (optional — exceptional cases only)",
                    value=last_fp.get("fp_comment", ""),
                    key=f"fp_comment_{fk}",
                )

            st.markdown("---")

            # Additional species input with autocomplete (multiselect)
            other_species_list = st.multiselect(
                "**Other species not listed above:**",
                options=all_species,
                default=last_fields.get("other_species_list", []),
                help=(
                    "Search and select additional species not in the "
                    "checklist above. Note vocalization type (Call/Song) in "
                    "the Comments field below if relevant."
                ),
                placeholder="Start typing to search...",
                key=f"other_species_{fk}",
            )

            st.markdown("---")

            # Confidence rating
            confidence_options = ["Low", "Moderate", "High"]
            prefilled_confidence = last_fields.get("user_confidence")
            user_confidence = st.radio(
                "**How confident are you in your annotations?**",
                options=confidence_options,
                index=(
                    confidence_options.index(prefilled_confidence)
                    if prefilled_confidence in confidence_options
                    else None
                ),
                horizontal=True,
                help=(
                    "Rate your overall confidence in the species identifications above"
                ),
            )

            st.markdown("---")

            # Notes/comments field — plain checkbox grid (no dropdown/popover)
            # so annotators can tick sounds in a single click each.
            st.markdown("#### 📝 Additional sounds")

            noise_classes = [
                "Loud foreground noise",
                "Rain",
                "Wind",
                "Water/Waves",
                "Other Environmental Noise",
                "Dog/Bark",
                "Insect/Cricket",
                "Amphibian/Frogs",
                "Other Animal",
                "Aircraft",
                "Traffic/Car",
                "Construction",
                "Human Voices",
                "Other Anthropogenic",
            ]
            prefilled_notes = last_fields.get("user_notes", [])
            user_notes = []
            mid = (len(noise_classes) + 1) // 2
            nc1, nc2 = st.columns(2)
            with nc1:
                for noise in noise_classes[:mid]:
                    if st.checkbox(
                        noise,
                        value=noise in prefilled_notes,
                        key=f"noise_{noise}_{fk}",
                    ):
                        user_notes.append(noise)
            with nc2:
                for noise in noise_classes[mid:]:
                    if st.checkbox(
                        noise,
                        value=noise in prefilled_notes,
                        key=f"noise_{noise}_{fk}",
                    ):
                        user_notes.append(noise)

            st.markdown("---")

            # Free-text comments field
            st.markdown("#### 💬 Comments")
            user_comments = st.text_area(
                "Add any additional comments or observations:",
                value=last_fields.get("user_comments", ""),
                placeholder=(
                    "E.g., 'Faint call in background', "
                    "'Multiple individuals', "
                    "'Uncertain due to noise'..."
                ),
                height=100,
                key=f"user_comments_{fk}",
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation",
                type="primary",
                use_container_width=True,
                shortcut="Ctrl+Enter",
            )

            if submitted:
                _handle_pro_validation_submission(
                    result,
                    selections,
                    species_only_selected,
                    none_of_above,
                    unidentified_bird,
                    other_species_list,
                    user_notes,
                    user_confidence,
                    user_comments,
                    vocalization_types,
                    species_data,
                    {
                        "fp_source": fp_source,
                        "fp_species": fp_species,
                        "fp_other_bird_text": fp_other_bird_text,
                        "fp_vocalization": fp_vocalization,
                        "fp_mechanism": fp_mechanism,
                        "fp_certainty": fp_certainty,
                        "fp_comment": fp_comment,
                    },
                    existing_record is not None,
                )


def _handle_pro_validation_submission(
    result,
    selections,
    species_only_selected,
    none_of_above,
    unidentified_bird,
    other_species_list,
    user_notes,
    user_confidence,
    user_comments,
    vocalization_types,
    species_data=None,
    fp_form_data=None,
    has_existing_record=False,
):
    """Handle Expert mode validation form submission."""
    if not user_confidence:
        st.error("Please rate your confidence before submitting.")
        return

    # --- Primary identification consistency check (prevent ambiguous /
    # incomplete submissions) ---
    # Exactly one of: a specific species marked present, "No bird species
    # audible", or "Unidentified bird present" must be chosen.
    has_species = bool(species_only_selected)
    has_none = bool(none_of_above)
    has_unidentified = bool(unidentified_bird)
    chosen_count = sum([has_species, has_none, has_unidentified])

    if chosen_count == 0:
        st.error(
            "Please mark at least one species, 'No bird species audible', or "
            "'Unidentified bird present' before submitting."
        )
        return

    if chosen_count > 1:
        st.error(
            "Please choose only one: specific species present, 'No bird "
            "species audible', or 'Unidentified bird present' — these are "
            "mutually exclusive."
        )
        return

    if has_none:
        selected_species = ["NONE_DETECTED"]
    elif has_unidentified:
        selected_species = ["UNIDENTIFIED_BIRD_PRESENT"]
    else:
        selected_species = species_only_selected

    # --- FP characterization consistency checks (prevent silent data loss) ---
    # The FP block's fields are only saved if the "Likely FP source/confuser"
    # dropdown is set AND at least one species was left unconfirmed. If either
    # holds without the other, block submission instead of silently dropping
    # whatever the annotator filled in.
    fp_source = (fp_form_data or {}).get("fp_source", "")
    fp_source_is_set = fp_source not in ("", "— Not a false positive / N/A —")
    rejected_species = [
        (species, conf_val)
        for species, conf_val in (species_data or [])
        if species not in selected_species
    ]
    fp_any_other_field_filled = any(
        [
            (fp_form_data or {}).get("fp_vocalization"),
            (fp_form_data or {}).get("fp_mechanism"),
            (fp_form_data or {}).get("fp_certainty"),
            ((fp_form_data or {}).get("fp_comment") or "").strip(),
            (fp_form_data or {}).get("fp_species"),
            ((fp_form_data or {}).get("fp_other_bird_text") or "").strip(),
        ]
    )

    if fp_source_is_set and not rejected_species:
        st.error(
            "🔬 You selected a 'Likely FP source/confuser' but every listed "
            "species is still confirmed. Uncheck the species this applies "
            "to (or check 'No bird species audible'), or reset the FP "
            "source back to '— Not a false positive / N/A —' if this clip "
            "isn't a false positive."
        )
        return

    if not fp_source_is_set and fp_any_other_field_filled:
        st.error(
            "🔬 You've filled in False Positive details but haven't selected "
            "a 'Likely FP source/confuser'. Please select one, or clear the "
            "False Positive fields if this clip isn't a false positive."
        )
        return

    is_fp_characterized = fp_source_is_set and bool(rejected_species)

    # Extract scientific names from multiselect
    # Format: "Common Name (Scientific Name)"
    additional_species = [
        species_str.split(" (")[-1].rstrip(")")
        if " (" in species_str and species_str.endswith(")")
        else species_str
        for species_str in other_species_list
    ]

    all_identified_species = selected_species + additional_species

    # Get arrays directly (DuckDB returns actual lists)
    birdnet_species = result.get("species_array", []) or []
    birdnet_confidences = result.get("confidence_array", []) or []

    # Look up site metadata from site_info.csv
    from database.queries import get_device_site_map

    deployment_id = result.get("deployment_id", "")
    device_site_map = get_device_site_map()
    # Try lookup by deployment_id first, then by device_id (last segment)
    site_info = device_site_map.get(deployment_id)
    if not site_info:
        device_id = (
            deployment_id.rsplit("_", 1)[-1] if "_" in deployment_id else deployment_id
        )
        site_info = device_site_map.get(device_id, {})

    # Prepare validation data
    validation_data = {
        "filename": result["filename"],
        "userID": selections["user_id"],
        "deployment_id": deployment_id,
        "country": site_info.get("country", ""),
        "site_name": site_info.get("site", ""),
        "cluster": site_info.get("cluster", ""),
        "birdnet_species_detected": birdnet_species,
        "birdnet_confidences": birdnet_confidences,
        "start_time": result["start_time"],
        "identified_species": all_identified_species,
        "species_count": len(all_identified_species),
        "vocalization_types": vocalization_types,
        "user_confidence": user_confidence,
        "user_notes": user_notes,
        "user_comments": user_comments,
        "timestamp": pd.Timestamp.now(),
        "is_update": has_existing_record,
        "is_cross_validation": False,
    }

    # Clip-level False Positive characterization. Applies to all
    # BirdNET-detected species that were rejected in this clip (left
    # unchecked, or covered by "No bird species audible"). Left blank when
    # the FP block wasn't filled in, so TP-only submissions simply have
    # empty fp_* columns in the same output file.
    fp_category, fp_detail = "", ""
    if is_fp_characterized:
        if fp_source.startswith("Specific bird species"):
            fp_category = "Bird"
            # Parse "Common Name (Scientific Name)" back to pure scientific
            # name, same as the "Other species not listed above" field, so
            # fp_source_detail stays consistent with fp_target_species
            # regardless of the selected display language.
            raw_fp_species = fp_form_data.get("fp_species") or ""
            fp_detail = (
                raw_fp_species.split(" (")[-1].rstrip(")")
                if " (" in raw_fp_species and raw_fp_species.endswith(")")
                else raw_fp_species
            )
        elif fp_source.startswith("Other bird"):
            fp_category = "Other bird"
            fp_detail = fp_form_data.get("fp_other_bird_text") or ""
        elif " — " in fp_source:
            fp_category, fp_detail = fp_source.split(" — ", 1)
        else:
            fp_category, fp_detail = fp_source, ""

    validation_data.update(
        {
            "fp_target_species": (
                [species for species, _ in rejected_species]
                if is_fp_characterized
                else []
            ),
            "fp_target_species_confidence": (
                [conf_val for _, conf_val in rejected_species]
                if is_fp_characterized
                else []
            ),
            "fp_source_category": fp_category,
            "fp_source_detail": fp_detail,
            "fp_confuser_vocalization": (
                (fp_form_data or {}).get("fp_vocalization", "")
                if is_fp_characterized
                else ""
            ),
            "fp_likely_mechanism": (
                (fp_form_data or {}).get("fp_mechanism", [])
                if is_fp_characterized
                else []
            ),
            "fp_certainty": (
                ((fp_form_data or {}).get("fp_certainty") or "")
                if is_fp_characterized
                else ""
            ),
            "fp_comment": (
                (fp_form_data or {}).get("fp_comment", "")
                if is_fp_characterized
                else ""
            ),
        }
    )

    # Save validation response (includes FP characterization columns)
    from utils import save_pro_validation_response

    save_pro_validation_response(validation_data)

    # Advance to the next clip in the list (clamped to the last index).
    clips = st.session_state.get("expert_full_clip_list") or []
    if clips:
        st.session_state.expert_clip_index = min(
            st.session_state.get("expert_clip_index", 0) + 1, len(clips) - 1
        )

    # Increment form key to reset all form fields
    st.session_state.expert_form_key = st.session_state.get("expert_form_key", 0) + 1

    if has_existing_record:
        st.toast("✅ Correction saved!")
    else:
        st.toast("✅ Annotation saved! Loading next clip...")
    st.rerun()
