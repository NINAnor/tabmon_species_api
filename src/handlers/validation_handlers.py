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


def render_pro_validation_form(result, selections):
    """
    Render the Expert mode validation form with species checklist.

    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Expert Validation")

        # Show remaining clips count from session state
        if (
            "expert_remaining_count" in st.session_state
            and st.session_state.expert_remaining_count is not None
        ):
            remaining = st.session_state.expert_remaining_count
            st.success("🎉 All clips validated!") if remaining == 0 else st.info(
                f"📊 Still **{remaining}** clips to annotate"
            )

        fk = st.session_state.get("expert_form_key", 0)

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
            language_code = selections.get("language_code", "Scientific_Name")
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
            all_species = _get_all_species_list(language_code)

            # Display checklist
            selected_species = []
            vocalization_types = {}
            for idx, (species, conf_val) in enumerate(species_data):
                display_name = scientific_to_display.get(species, species)
                col_check, col_vocal = st.columns([3, 2])
                with col_check:
                    checked = st.checkbox(
                        f"{display_name} (Birdnet conf: {conf_val:.2f})",
                        key=f"species_{idx}_{fk}",
                    )
                with col_vocal:
                    vocal_type = st.selectbox(
                        "Vocalization",
                        options=["", "Call", "Song", "Other/Unknown"],
                        key=f"vocal_{idx}_{fk}",
                        label_visibility="collapsed",
                    )
                if checked:
                    selected_species.append(species)
                    if vocal_type:
                        vocalization_types[species] = vocal_type

            none_of_above = st.checkbox(
                "❌ None of the above species are present",
                key=f"none_of_above_{fk}",
                help="Check this if you cannot hear any of the species listed above",
            )

            if none_of_above:
                selected_species = ["NONE_DETECTED"]

            # Keyboard shortcuts: Ctrl+1 / Ctrl+2 toggle the top two species
            # checkboxes, Ctrl+3 toggles "None of the above". Guarded so we
            # never reference a checkbox key that doesn't exist for clips
            # with fewer than 2 BirdNET detections.
            shortcut_bindings = {}
            if len(species_data) >= 1:
                shortcut_bindings[f"species_0_{fk}"] = "ctrl+1"
            if len(species_data) >= 2:
                shortcut_bindings[f"species_1_{fk}"] = "ctrl+2"
            shortcut_bindings[f"none_of_above_{fk}"] = "ctrl+3"
            add_shortcuts(**shortcut_bindings)

            # Clip-level False Positive characterization — applies whenever
            # one or more BirdNET-detected species were rejected (left
            # unchecked, or covered by "None of the above"). One block per
            # clip rather than per species, since the FP source/confuser
            # typically describes the whole clip, not an individual row.
            with st.expander("🔬 Characterize as False Positive (optional)"):
                fp_source = st.selectbox(
                    "Likely FP source/confuser",
                    options=[
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
                    ],
                    key=f"fp_source_{fk}",
                )
                fp_species = st.selectbox(
                    "If 'Specific bird species': which species?",
                    options=all_species,
                    index=None,
                    placeholder="Start typing to search...",
                    key=f"fp_species_{fk}",
                )
                fp_other_bird_text = st.text_input(
                    "If 'Other bird': describe "
                    "(e.g. 'lark, sparrow, finch, etc.')",
                    key=f"fp_other_bird_{fk}",
                )
                fp_vocalization = st.selectbox(
                    "Vocalization type of confuser (optional)",
                    options=["", "Song", "Call", "Other/Unknown"],
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
                fp_mechanism = [
                    mechanism
                    for mechanism in mechanism_options
                    if st.checkbox(mechanism, key=f"fp_mechanism_{mechanism}_{fk}")
                ]
                fp_certainty = st.radio(
                    "Certainty",
                    options=["Low", "Medium", "High"],
                    index=None,
                    horizontal=True,
                    key=f"fp_certainty_{fk}",
                )
                fp_comment = st.text_input(
                    "Comment (optional — exceptional cases only)",
                    key=f"fp_comment_{fk}",
                )

            st.markdown("---")

            # Additional species input with autocomplete (multiselect)
            other_species_list = st.multiselect(
                "**Other species not listed above:**",
                options=all_species,
                default=[],
                help="Search and select additional species not in the checklist above",
                placeholder="Start typing to search...",
                key=f"other_species_{fk}",
            )

            st.markdown("---")

            # Confidence rating
            user_confidence = st.radio(
                "**How confident are you in your annotations?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                help=(
                    "Rate your overall confidence in the species identifications above"
                ),
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation",
                type="primary",
                use_container_width=True,
                shortcut="Ctrl+Enter",
            )

            st.markdown("---")

            # Notes/comments field
            st.markdown("#### 📝 Additional sounds")

            noise_classes = [
                "Loud foreground noise",
                "Environmental — Rain",
                "Environmental — Wind",
                "Environmental — Water/Waves",
                "Environmental — Other Environmental Noise",
                "Other animal — Dog/Bark",
                "Other animal — Insect/Cricket",
                "Other animal — Amphibian/Frogs",
                "Other animal — Other Animal",
                "Anthropogenic — Aircraft",
                "Anthropogenic — Traffic/Car",
                "Anthropogenic — Construction",
                "Anthropogenic — Human Voices",
                "Anthropogenic — Other Anthropogenic",
            ]
            user_notes = st.multiselect(
                "Select any additional background sounds heard in this clip:",
                options=noise_classes,
                default=[],
                placeholder="Start typing to search...",
                key=f"user_notes_{fk}",
            )

            st.markdown("---")

            # Free-text comments field
            st.markdown("#### 💬 Comments")
            user_comments = st.text_area(
                "Add any additional comments or observations:",
                placeholder=(
                    "E.g., 'Faint call in background', "
                    "'Multiple individuals', "
                    "'Uncertain due to noise'..."
                ),
                height=100,
                key=f"user_comments_{fk}",
            )

            if submitted:
                _handle_pro_validation_submission(
                    result,
                    selections,
                    selected_species,
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
                )


def _handle_pro_validation_submission(
    result,
    selections,
    selected_species,
    other_species_list,
    user_notes,
    user_confidence,
    user_comments,
    vocalization_types,
    species_data=None,
    fp_form_data=None,
):
    """Handle Expert mode validation form submission."""
    if not user_confidence:
        st.error("Please rate your confidence before submitting.")
        return

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
            "to (or check 'None of the above'), or reset the FP source back "
            "to '— Not a false positive / N/A —' if this clip isn't a false "
            "positive."
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
    }

    # Clip-level False Positive characterization. Applies to all
    # BirdNET-detected species that were rejected in this clip (left
    # unchecked, or covered by "None of the above"). Left blank when the
    # FP block wasn't filled in, so TP-only submissions simply have empty
    # fp_* columns in the same output file.
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

    # Clear current clip to force loading a new one
    st.session_state.expert_current_clip = None

    # Increment form key to reset all form fields
    st.session_state.expert_form_key = st.session_state.get("expert_form_key", 0) + 1

    st.toast("✅ Annotation saved! Loading next clip...")
    st.rerun()
