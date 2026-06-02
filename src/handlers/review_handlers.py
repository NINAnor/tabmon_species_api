"""Review Mode Handlers - filter controls for browsing validated clips."""

import streamlit as st

from database.review_queries import (
    get_available_validators,
    get_filtered_validations,
    load_dataset_validations,
)


def render_review_filters(dataset_path, language_code):
    """Render filter controls as a main-page expander for review mode.

    Args:
        dataset_path: S3 path to the parquet dataset
        language_code: Language code for species display names

    Returns True if filters are applied and results are available.
    """
    with st.expander("🔍 **Review Filters** — Search previously validated clips", expanded=st.session_state.review_results is None):
        # Load validations to determine available filter options
        validations_df = load_dataset_validations(dataset_path)

        if validations_df.empty:
            st.warning("No validated clips found for this dataset yet.")
            return False

        st.caption(f"📋 {len(validations_df)} validated clips available")

        # Lay out filters in columns for compact display
        col1, col2, col3 = st.columns(3)

        with col1:
            # Date range filter
            min_date = validations_df["recording_date"].min()
            max_date = validations_df["recording_date"].max()

            if min_date is not None and max_date is not None and not (hasattr(min_date, 'year') and min_date != min_date):
                date_range = st.date_input(
                    "🗓️ Recording Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    help="Filter by recording date range",
                )
                # date_input returns tuple when range, single date otherwise
                if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                    selected_date_range = date_range
                else:
                    selected_date_range = None
            else:
                selected_date_range = None
                st.info("No date information available")

            # BirdNET confidence range
            confidence_range = st.slider(
                "🤖 BirdNET Confidence",
                min_value=0.0,
                max_value=1.0,
                value=(0.0, 1.0),
                step=0.05,
                help="Filter by max BirdNET confidence score",
            )

        with col2:
            # Site filter
            available_sites = sorted(
                validations_df["site_name"]
                .dropna()
                .astype(str)
                .loc[lambda s: s.str.strip() != ""]
                .unique()
                .tolist()
            )
            selected_sites = st.multiselect(
                "📍 Site",
                options=available_sites,
                default=[],
                help="Filter by recording site",
                placeholder="All sites",
            )

            # Expert confidence class
            confidence_classes = st.multiselect(
                "🎯 Expert Confidence",
                options=["Low", "Moderate", "High"],
                default=[],
                help="Filter by the expert's confidence rating",
                placeholder="All levels",
            )

            # Species filter
            all_species_in_validations = set()
            for species_str in validations_df["birdnet_species_detected"].dropna():
                for species in str(species_str).split("|"):
                    species = species.strip()
                    if species:
                        all_species_in_validations.add(species)

            species_options = sorted(all_species_in_validations)

            if language_code != "Scientific_Name":
                from utils import get_species_display_names

                display_map = get_species_display_names(
                    species_options, language_code
                )
                display_to_scientific = display_map
                display_options = sorted(display_map.keys())
            else:
                display_options = species_options
                display_to_scientific = {sp: sp for sp in species_options}

            selected_display_species = st.multiselect(
                "🐦 Species",
                options=display_options,
                default=[],
                help="Filter by species (detected or identified)",
                placeholder="All species",
            )
            selected_species = (
                [display_to_scientific[dn] for dn in selected_display_species]
                or None
            )

        with col3:
            # Validator filter
            available_validators = get_available_validators(dataset_path)
            validator_options = ["All validators"] + available_validators

            selected_validator = st.selectbox(
                "👤 Validator",
                options=validator_options,
                help="Filter by who performed the validation",
            )
            validator_filter = (
                None
                if selected_validator == "All validators"
                else selected_validator
            )

            # Comments filter
            has_comments = st.checkbox(
                "💬 Has comments only",
                value=False,
                help="Show only clips with comments",
            )

            comment_search = st.text_input(
                "🔎 Search comments",
                value="",
                help="Search for text in comments",
                placeholder="e.g. 'uncertain'",
            )

            # Vocalization type filter
            vocalization_type = st.selectbox(
                "🎵 Vocalization Type",
                options=["All", "Call", "Song"],
                help="Filter by vocalization type identified by expert",
            )
            selected_vocalization = (
                None if vocalization_type == "All" else vocalization_type
            )

        # Action buttons row
        col_apply, col_reset, col_status = st.columns([1, 1, 2])

        with col_apply:
            if st.button(
                "🔍 Apply Filters", type="primary", use_container_width=True
            ):
                filters = {
                    "date_range": selected_date_range,
                    "confidence_range": confidence_range,
                    "user_confidence": confidence_classes or None,
                    "has_comments": has_comments,
                    "comment_search": comment_search,
                    "species": selected_species,
                    "validator": validator_filter,
                    "vocalization_type": selected_vocalization,
                    "sites": selected_sites or None,
                }

                results_df = get_filtered_validations(dataset_path, filters)
                st.session_state.review_results = results_df
                st.session_state.review_current_index = 0
                st.session_state.review_filters = filters
                st.rerun()

        with col_reset:
            if st.button("🔄 Reset", use_container_width=True):
                st.session_state.review_results = None
                st.session_state.review_current_index = 0
                st.session_state.review_filters = {}
                load_dataset_validations.clear()
                st.rerun()

        with col_status:
            if st.session_state.review_results is not None:
                count = len(st.session_state.review_results)
                current_idx = st.session_state.review_current_index
                if count > 0:
                    st.success(
                        f"✅ **{count}** clips found — "
                        f"viewing {current_idx + 1} of {count}"
                    )
                else:
                    st.warning("No clips match the current filters.")

    return (
        st.session_state.review_results is not None
        and len(st.session_state.review_results) > 0
    )
