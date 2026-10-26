"""Expert Mode Validation Handlers - checklist of top species."""

import pandas as pd
import streamlit as st

from ui.ui_shortcuts import add_form_shortcuts

from session.session_manager import bump_expert_form_key
from utils import (
    get_species_display_names,
    load_species_translations,
    parse_list_cell,
)

# Sentinels written to the identified_species column for "no species"
# statements. NONE_DETECTED: none of the BirdNET-detected species are
# actually present (other birds may be audible — list them below).
# UNIDENTIFIED_BIRD_PRESENT: a bird call/song is audible but its species
# cannot be determined. NO_BIRD_AUDIBLE: complete silence, no bird of any
# kind is audible.
NO_BIRD_AUDIBLE = "NO_BIRD_AUDIBLE"

# FP source checkbox labels (multi-select). "Specific bird species" and
# "Other bird (unidentified)" additionally reveal a species picker / text
# input when ticked.
FP_SOURCE_SPECIES = "Specific bird species"
FP_SOURCE_OTHER_BIRD = "Other bird (unidentified)"
FP_SOURCE_OPTIONS = [
    FP_SOURCE_SPECIES,
    FP_SOURCE_OTHER_BIRD,
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

# Checkbox label -> (fp_source_category, fp_source_detail) pair, except the
# two special sources handled separately (species picker / free text).
_FP_CATEGORY_DETAIL_BY_LABEL = {
    "Other animal — Amphibian/Frogs": ("Other animal", "Amphibian/Frogs"),
    "Other animal — Insect/Cricket": ("Other animal", "Insect/Cricket"),
    "Other animal — Other Animal": ("Other animal", "Other Animal"),
    "Environmental — Rain": ("Environmental", "Rain"),
    "Environmental — Wind": ("Environmental", "Wind"),
    "Environmental — Other Environmental Noise": (
        "Environmental",
        "Other Environmental Noise",
    ),
    "Anthropogenic — Aircraft": ("Anthropogenic", "Aircraft"),
    "Anthropogenic — Traffic/Car": ("Anthropogenic", "Traffic/Car"),
    "Anthropogenic — Other Anthropogenic": (
        "Anthropogenic",
        "Other Anthropogenic",
    ),
    "Unclear": ("Unclear", ""),
}

FP_MECHANISM_OPTIONS = [
    "Acoustic similarity",
    "Partial vocalisation",
    "Overlapping signal",
    "Low SNR / masking",
    "Faint/distant vocalization",
    "Other / unclear",
]

FP_CERTAINTY_OPTIONS = ["Low", "Medium", "High"]


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


def _extract_scientific(display_name):
    """Parse "Common Name (Scientific Name)" back to the pure scientific name.

    Non-scientific strings (free text like 'lark, sparrow') pass through
    unchanged.
    """
    if (
        not display_name
        or " (" not in display_name
        or not display_name.endswith(")")
    ):
        return display_name
    return display_name.split(" (")[-1].rstrip(")")


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


_FP_CURSOR_HELP = (
    "CTE placeholder — replaced below when the functions are defined."
)


def render_fp_characterization_fragment(prefix, fk, all_species, last_fp):
    """False-Positive characterization block, rendered OUTSIDE the form.

    Wrapped in @st.fragment so toggling the main checkbox reruns only this
    block (instant open/close of the FP fields, no full-page rerun, audio
    keeps playing). Widget values are collected at submit time via
    `_collect_fp_widget_values`.

    Args:
        prefix: Key prefix ("fp" in the validation form, "xfp" in review).
        fk: Form key so all widget keys reset when the clip changes.
        all_species: Options for the confuser-species picker.
        last_fp: Prefill dict (see `_parse_validation_record_for_prefill`).
    """
    st.markdown("#### 🔬 False-Positive assessment (optional)")
    st.checkbox(
        "This clip's BirdNET detections are false positives",
        value=last_fp.get("assessment", False),
        key=f"{prefix}_assessed_{fk}",
        help=(
            "Tick if at least one BirdNET detection above was wrong and you "
            "want to characterize what confused the model"
        ),
    )
    if not st.session_state.get(f"{prefix}_assessed_{fk}", False):
        return

    with st.container(border=True):
        st.markdown(
            "**What is the likely source/confuser?** (tick all that apply)"
        )
        prefill_sources = last_fp.get("ticked_sources") or []
        half = (len(FP_SOURCE_OPTIONS) + 1) // 2
        col_a, col_b = st.columns(2)
        chunks = (FP_SOURCE_OPTIONS[:half], FP_SOURCE_OPTIONS[half:])
        for column, chunk in zip((col_a, col_b), chunks, strict=True):
            with column:
                for sig in chunk:
                    st.checkbox(
                        sig,
                        value=sig in prefill_sources,
                        key=f"{prefix}_src_{sig}_{fk}",
                    )

        ticked_sources = [
            sig
            for sig in FP_SOURCE_OPTIONS
            if st.session_state.get(f"{prefix}_src_{sig}_{fk}", False)
        ]

        if FP_SOURCE_SPECIES in ticked_sources:
            prefilled_fp_species = last_fp.get("fp_species")
            st.selectbox(
                "If 'Specific bird species': which species?",
                options=all_species,
                index=(
                    all_species.index(prefilled_fp_species)
                    if prefilled_fp_species in all_species
                    else None
                ),
                placeholder="Start typing to search...",
                key=f"{prefix}_species_{fk}",
            )
        if FP_SOURCE_OTHER_BIRD in ticked_sources:
            st.text_input(
                "If 'Other bird (unidentified)': describe "
                "(e.g. 'lark, sparrow, finch, etc.')",
                value=last_fp.get("fp_other_bird_text", ""),
                key=f"{prefix}_other_bird_{fk}",
            )

        fp_vocal_options = ["", "Song", "Call", "Other/Unknown"]
        prefilled_fp_vocal = last_fp.get("fp_vocalization", "")
        st.selectbox(
            "Vocalization type of confuser (optional)",
            options=fp_vocal_options,
            index=(
                fp_vocal_options.index(prefilled_fp_vocal)
                if prefilled_fp_vocal in fp_vocal_options
                else 0
            ),
            key=f"{prefix}_vocal_{fk}",
        )

        st.markdown("**Likely confusion mechanism:**")
        prefilled_mechanisms = last_fp.get("fp_mechanism", []) or []
        mid = (len(FP_MECHANISM_OPTIONS) + 1) // 2
        m1, m2 = st.columns(2)
        for column, chunk in (
            (m1, FP_MECHANISM_OPTIONS[:mid]),
            (m2, FP_MECHANISM_OPTIONS[mid:]),
        ):
            with column:
                for mechanism in chunk:
                    st.checkbox(
                        mechanism,
                        value=mechanism in prefilled_mechanisms,
                        key=f"{prefix}_mech_{mechanism}_{fk}",
                    )

        prefilled_fp_certainty = last_fp.get("fp_certainty")
        st.radio(
            "Certainty",
            options=FP_CERTAINTY_OPTIONS,
            index=(
                FP_CERTAINTY_OPTIONS.index(prefilled_fp_certainty)
                if prefilled_fp_certainty in FP_CERTAINTY_OPTIONS
                else None
            ),
            horizontal=True,
            key=f"{prefix}_certainty_{fk}",
        )
        st.text_input(
            "Comment (optional — exceptional cases only)",
            value=last_fp.get("fp_comment", ""),
            key=f"{prefix}_comment_{fk}",
        )


def render_confidence_fragment(prefix, fk, initial=None):
    """Annotation-confidence selector, rendered OUTSIDE the form.

    Three buttons with native keyboard shortcuts (q/w/e). Native button
    shortcuts ignore text fields, and the fragment rerun is local, so
    playback is never interrupted. Clicks must not fall inside the form
    (buttons inside st.form would submit it), and the selection is read
    from session state at submit time.

    The session value is written with setdefault so the prefill only
    applies on the fragment's first render for this form key.

    Args:
        prefix: Session-key prefix ("expert" or "review").
        fk: Form key so the selection resets when the clip changes.
        initial: Confidence to prefill on first render (None otherwise).
    """
    session_key = f"{prefix}_confidence_val_{fk}"
    st.session_state.setdefault(session_key, initial)
    selected = st.session_state[session_key]

    st.markdown("**Confidence:**")
    col_low, col_moderate, col_high = st.columns(3)
    with col_low:
        if st.button(
            "✅ Low" if selected == "Low" else "Low",
            key=f"{prefix}_conf_low_{fk}",
            shortcut="q",
            use_container_width=True,
        ):
            st.session_state[session_key] = "Low"
    with col_moderate:
        if st.button(
            "✅ Moderate" if selected == "Moderate" else "Moderate",
            key=f"{prefix}_conf_moderate_{fk}",
            shortcut="w",
            use_container_width=True,
        ):
            st.session_state[session_key] = "Moderate"
    with col_high:
        if st.button(
            "✅ High" if selected == "High" else "High",
            key=f"{prefix}_conf_high_{fk}",
            shortcut="e",
            use_container_width=True,
        ):
            st.session_state[session_key] = "High"


def _collect_fp_widget_values(prefix, fk):
    """Read the current FP characterization widget values from session state.

    Complements `render_fp_characterization_fragment`, which lives outside
    the form — its widgets can't be passed as form values, so the submit
    handler reads them here. Species/other-bird detail are only read when
    their parent source checkbox is ticked (the widget may not exist
    otherwise).
    """
    ticked = [
        sig
        for sig in FP_SOURCE_OPTIONS
        if st.session_state.get(f"{prefix}_src_{sig}_{fk}", False)
    ]
    values = {
        "assessment": st.session_state.get(f"{prefix}_assessed_{fk}", False),
        "ticked_sources": ticked,
        "fp_species": None,
        "fp_other_bird_text": "",
        "fp_vocalization": st.session_state.get(f"{prefix}_vocal_{fk}") or "",
        "fp_mechanism": [
            mechanism
            for mechanism in FP_MECHANISM_OPTIONS
            if st.session_state.get(f"{prefix}_mech_{mechanism}_{fk}", False)
        ],
        "fp_certainty": st.session_state.get(f"{prefix}_certainty_{fk}"),
        "fp_comment": st.session_state.get(f"{prefix}_comment_{fk}") or "",
    }
    if FP_SOURCE_SPECIES in ticked:
        values["fp_species"] = st.session_state.get(f"{prefix}_species_{fk}")
    if FP_SOURCE_OTHER_BIRD in ticked:
        values["fp_other_bird_text"] = (
            st.session_state.get(f"{prefix}_other_bird_{fk}") or ""
        )
    return values


def _empty_fp_columns():
    """Default (unfilled) fp_* column values for the validation record."""
    return {
        "fp_target_species": [],
        "fp_target_species_confidence": [],
        "fp_source_category": [],
        "fp_source_detail": [],
        "fp_confuser_vocalization": "",
        "fp_likely_mechanism": [],
        "fp_certainty": "",
        "fp_comment": "",
        "is_fp_assessment": False,
    }


def _build_fp_columns(fp_values, rejected_species, has_detections):
    """Validate the FP widget values and build the fp_* CSV columns.

    Returns (error_message_or_None, fp_columns_or_None). On success the
    columns are lists (pipe-joined later on save) such that fp_source_category
    and fp_source_detail pair up positionally.

    Args:
        fp_values: dict from `_collect_fp_widget_values`.
        rejected_species: [(species, confidence)] for every BirdNET detection
            the annotator did NOT confirm.
        has_detections: whether the clip had any BirdNET detections at all
            (a FP assessment of a clip without detections is pointless and
            blocked).
    """
    fp_values = fp_values or {}
    assessment = bool(fp_values.get("assessment"))
    ticked = fp_values.get("ticked_sources", []) or []
    any_field_filled = any(
        [
            ticked,
            fp_values.get("fp_species"),
            (fp_values.get("fp_other_bird_text") or "").strip(),
            fp_values.get("fp_vocalization"),
            fp_values.get("fp_mechanism"),
            fp_values.get("fp_certainty"),
            (fp_values.get("fp_comment") or "").strip(),
        ]
    )

    if (not assessment) and any_field_filled:
        return (
            "🔬 You filled in False-Positive details but didn't tick "
            "'This clip's BirdNET detections are false positives'. Tick the "
            "box, or clear the False-Positive fields if this clip isn't a "
            "false positive.",
            None,
        )

    if not assessment:
        return None, _empty_fp_columns()

    if has_detections and not rejected_species:
        return (
            "🔬 You indicated a False-Positive assessment but every listed "
            "species is still confirmed. Uncheck the species that were "
            "wrongly detected (or check 'None of the listed species are "
            "present'), or untick the False-Positive assessment box if this "
            "clip isn't a false positive.",
            None,
        )
    if FP_SOURCE_SPECIES in ticked and not (fp_values.get("fp_species") or None):
        return (
            "🔬 You ticked 'Specific bird species' but didn't select which "
            "species is the actual source of the false positive.",
            None,
        )

    if FP_SOURCE_OTHER_BIRD in ticked and not (
        fp_values.get("fp_other_bird_text") or ""
    ).strip():
        return (
            "🔬 You ticked 'Other bird (unidentified)' — please describe it "
            "(which family/type of bird did you hear?).",
            None,
        )

    categories = []
    details = []
    for sig in ticked:
        if sig == FP_SOURCE_SPECIES:
            categories.append("Bird")
            details.append(_extract_scientific(fp_values.get("fp_species")))
        elif sig == FP_SOURCE_OTHER_BIRD:
            categories.append("Other bird")
            details.append(fp_values.get("fp_other_bird_text", "").strip())
        else:
            category, detail = _FP_CATEGORY_DETAIL_BY_LABEL.get(sig, (sig, ""))
            categories.append(category)
            details.append(detail)

    return None, {
        "fp_target_species": [species for species, _ in rejected_species],
        "fp_target_species_confidence": [conf for _, conf in rejected_species],
        "fp_source_category": categories,
        "fp_source_detail": details,
        "fp_confuser_vocalization": fp_values.get("fp_vocalization") or "",
        "fp_likely_mechanism": fp_values.get("fp_mechanism") or [],
        "fp_certainty": fp_values.get("fp_certainty") or "",
        "fp_comment": fp_values.get("fp_comment") or "",
        "is_fp_assessment": True,
    }


def _parse_validation_record_for_prefill(record, language_code, all_species):
    """Convert a saved validation CSV row back into the same shape used to
    pre-fill the form's widgets (species, vocalization, FP block, etc.).
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
    no_bird_audible = NO_BIRD_AUDIBLE in identified_list
    species_only_selected = [s for s in identified_list if s in detected_list]
    other_species_raw = [
        s
        for s in identified_list
        if s not in detected_list
        and s
        not in ("NONE_DETECTED", "UNIDENTIFIED_BIRD_PRESENT", NO_BIRD_AUDIBLE)
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

    # Reconstruct the FP source checkboxes from the saved
    # fp_source_category / fp_source_detail columns (list cells; older
    # records have a single plain value, handled identically).
    fp_categories = parse_list_cell(record.get("fp_source_category"))
    fp_details = parse_list_cell(record.get("fp_source_detail"))
    fp_species_raw = ""
    fp_other_bird_text = ""
    ticked_sources = []
    for idx, category in enumerate(fp_categories):
        detail = fp_details[idx] if idx < len(fp_details) else ""
        if category == "Bird":
            ticked_sources.append(FP_SOURCE_SPECIES)
            fp_species_raw = detail
        elif category == "Other bird":
            ticked_sources.append(FP_SOURCE_OTHER_BIRD)
            fp_other_bird_text = detail
        else:
            for sig, (mapped_cat, mapped_detail) in (
                _FP_CATEGORY_DETAIL_BY_LABEL.items()
            ):
                if mapped_cat == category and mapped_detail == detail:
                    ticked_sources.append(sig)
                    break

    fp_species = _scientific_to_display_option(
        fp_species_raw, language_code, all_species
    )

    fp_mechanism_str = _clean(record.get("fp_likely_mechanism"))
    fp_mechanism = [s.strip() for s in fp_mechanism_str.split("|") if s.strip()]

    fp_certainty = _clean(record.get("fp_certainty")) or None
    fp_vocalization = _clean(record.get("fp_confuser_vocalization"))
    has_fp_content = any(
        [
            ticked_sources,
            fp_mechanism,
            fp_vocalization,
            fp_certainty,
            _clean(record.get("fp_comment")),
        ]
    )

    return {
        "species_only_selected": species_only_selected,
        "vocalization_types": vocalization_types,
        "none_of_above": none_of_above,
        "no_bird_audible": no_bird_audible,
        "unidentified_bird": unidentified_bird,
        "other_species_list": other_species_list,
        "user_confidence": _clean(record.get("user_confidence")) or None,
        "user_notes": user_notes,
        "user_comments": _clean(record.get("user_comments")),
        "fp_form_data": {
            "assessment": bool(has_fp_content),
            "ticked_sources": ticked_sources,
            "fp_species": fp_species,
            "fp_other_bird_text": fp_other_bird_text,
            "fp_vocalization": fp_vocalization,
            "fp_mechanism": fp_mechanism,
            "fp_certainty": fp_certainty,
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

        # Clip-level False-Positive characterization — rendered ABOVE the
        # form inside a fragment so that ticking the assessment checkbox
        # instantly opens the FP fields (rerunning only the fragment, without
        # a page reload or audio restart). Applies to the BirdNET-detected
        # species that were rejected (left unchecked, or covered by 'None of
        # the listed species are present').
        render_fp_characterization_fragment(
            "fp", fk, all_species_for_prefill, last_fp
        )

        render_confidence_fragment("expert", fk, last_fields.get("user_confidence"))

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
            # confuser search above)
            all_species = all_species_for_prefill

            # Display checklist. Choosing a vocalization type alone (without
            # also checking the box) is enough to mark a species present —
            # the checkbox remains available for the "1" shortcut and for
            # marking presence without committing to Call/Song.
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
                "❌ None of the listed species are present",
                value=last_fields.get("none_of_above", False),
                key=f"none_of_above_{fk}",
                help=(
                    "Check this if none of the BirdNET-detected species are "
                    "actually present. If you hear other birds, select them "
                    "in the 'Other species not listed above' field below."
                ),
            )

            no_bird_audible = st.checkbox(
                "🕊️ No bird species audible",
                value=last_fields.get("no_bird_audible", False),
                key=f"no_bird_audible_{fk}",
                help=(
                    "Check this if no bird of any kind is audible in this "
                    "clip at all"
                ),
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

            # Keyboard shortcuts: "1" toggles the top (highest-confidence)
            # species checkbox, "2" toggles "None of the listed species are
            # present", "3" toggles "No bird species audible", "4" toggles
            # "Unidentified bird present". Guarded so we never reference a
            # checkbox key that doesn't exist for clips with no BirdNET
            # detections. Plain keys require the input-field guard from
            # add_form_shortcuts (typing "1"/"2"/"3"/"4" in text fields must
            # not toggle anything).
            shortcut_bindings = {}
            if len(species_data) >= 1:
                shortcut_bindings[f"species_0_{fk}"] = "1"
            shortcut_bindings[f"none_of_above_{fk}"] = "2"
            shortcut_bindings[f"no_bird_audible_{fk}"] = "3"
            shortcut_bindings[f"unidentified_bird_{fk}"] = "4"
            add_form_shortcuts(**shortcut_bindings)

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
                shortcut="Enter",
            )

            if submitted:
                _handle_pro_validation_submission(
                    result,
                    selections,
                    species_only_selected,
                    none_of_above,
                    no_bird_audible,
                    unidentified_bird,
                    other_species_list,
                    user_notes,
                    st.session_state.get(f"expert_confidence_val_{fk}"),
                    user_comments,
                    vocalization_types,
                    species_data,
                    _collect_fp_widget_values("fp", fk),
                    existing_record is not None,
                )


def _handle_pro_validation_submission(
    result,
    selections,
    species_only_selected,
    none_of_above,
    no_bird_audible,
    unidentified_bird,
    other_species_list,
    user_notes,
    user_confidence,
    user_comments,
    vocalization_types,
    species_data=None,
    fp_values=None,
    has_existing_record=False,
):
    """Handle Expert mode validation form submission."""
    # Confidence gating (user feedback): the submission is blocked only if
    # NEITHER the annotation confidence NOR the FP block's certainty is
    # selected — either one is sufficient. If only the FP certainty was
    # chosen, user_confidence stays blank in the CSV (the two are distinct
    # and are never blended).
    fp_certainty = (fp_values or {}).get("fp_certainty")
    if not user_confidence and not fp_certainty:
        st.error(
            "Please rate your confidence in the annotation above, or the "
            "certainty of the False-Positive assessment."
        )
        return

    # --- Primary identification: relaxed on purpose (user feedback) ---
    # Confidence is the only hard requirement. A submission is valid if it
    # carries at least one piece of annotation information: confirmed
    # species, "None of the listed species are present", "No bird species
    # audible", "Unidentified bird present", additional non-BirdNET
    # species, or a False-Positive assessment. Only genuinely contradictory
    # input is blocked.
    has_species = bool(species_only_selected)
    has_none = bool(none_of_above)
    has_silence = bool(no_bird_audible)
    has_unidentified = bool(unidentified_bird)
    has_other = bool(other_species_list)
    fp_assessment = bool((fp_values or {}).get("assessment"))

    if has_none and has_species:
        st.error(
            "Contradictory annotation: you both confirmed BirdNET species "
            "and checked 'None of the listed species are present'. Untick "
            "one or the other."
        )
        return

    if has_silence and any([has_species, has_unidentified, has_other]):
        st.error(
            "Contradictory annotation: 'No bird species audible' means no "
            "bird of any kind is audible, but you also marked birds above. "
            "Untick the contradictory options."
        )
        return

    if not any(
        [
            has_species,
            has_none,
            has_silence,
            has_unidentified,
            has_other,
            fp_assessment,
        ]
    ):
        st.error(
            "Please annotate this clip: mark which species you hear, check "
            "'None of the listed species are present', 'No bird species "
            "audible' or 'Unidentified bird present', add species not "
            "listed above, or tick the False-Positive assessment box."
        )
        return

    selected_species = list(species_only_selected)
    if has_none:
        selected_species.append("NONE_DETECTED")
    if has_silence:
        selected_species.append(NO_BIRD_AUDIBLE)
    if has_unidentified:
        selected_species.append("UNIDENTIFIED_BIRD_PRESENT")

    # --- FP characterization consistency checks (prevent silent data loss) ---
    # The FP block's fields are only saved when the FP assessment box is
    # ticked AND at least one species was left unconfirmed. If either holds
    # without the other, block submission instead of silently dropping
    # whatever the annotator filled in.
    rejected_species = [
        (species, conf_val)
        for species, conf_val in (species_data or [])
        if species not in species_only_selected
    ]
    has_detections = bool(species_data)
    error, fp_columns = _build_fp_columns(
        fp_values, rejected_species, has_detections
    )
    if error:
        st.error(error)
        return

    # Extract scientific names from multiselect
    # Format: "Common Name (Scientific Name)"
    additional_species = [_extract_scientific(s) for s in other_species_list]

    all_identified_species = selected_species + additional_species

    # Get arrays directly (DuckDB returns actual lists)
    birdnet_species = result.get("species_array", []) or []
    birdnet_confidences = result.get("confidence_array", []) or []

    # Look up site metadata from site_info.csv
    from database.queries import get_site_info_for_deployment

    deployment_id = result.get("deployment_id", "")
    site_info = get_site_info_for_deployment(deployment_id)

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
        "is_fp_assessment": False,
    }

    # Clip-level False Positive characterization (built by
    # _build_fp_columns). Applies to all BirdNET-detected species that were
    # rejected in this clip (left unchecked, or covered by 'None of the
    # listed species are present'). Left blank when the FP box wasn't
    # ticked, so TP-only submissions simply have empty fp_* columns in the
    # same output file.
    validation_data.update(fp_columns)

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
    bump_expert_form_key()

    if has_existing_record:
        st.toast("✅ Correction saved!")
    else:
        st.toast("✅ Annotation saved! Loading next clip...")
    st.rerun()
