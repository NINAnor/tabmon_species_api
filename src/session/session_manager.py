"""Expert Mode Session Management."""

import streamlit as st

from session.session_utils import (
    check_params_changed,
    init_base_session,
    init_state_vars,
)


def initialize_pro_session():
    """Initialize Expert mode session state variables."""
    init_base_session()
    init_state_vars(
        "expert_",
        {
            "clip_params": None,
            "authenticated": False,
            "user_id": None,
            "selected_dataset": None,
            "full_clip_list": None,
            "clip_index": 0,
        },
    )
    initialize_review_session()


def initialize_review_session():
    """Initialize review mode session state variables."""
    init_state_vars(
        "review_",
        {
            "mode_active": False,
            "results": None,
            "current_index": 0,
            "filters": {},
            "form_key": 0,
        },
    )


def clear_pro_clip_state():
    """Clear all Expert mode clip-related state and caches."""
    st.session_state.expert_full_clip_list = None
    st.session_state.expert_clip_index = 0
    st.session_state.expert_clip_params = None

    from database.queries import (
        get_assigned_clips_for_user,
        get_user_validation_records,
    )

    get_assigned_clips_for_user.clear()
    get_user_validation_records.clear()


def get_or_load_pro_clip(selections):
    """Get the clip at the current position in the user's full assigned list.

    Clips are served via true, stable position-based navigation rather than
    a prefetch queue: the full ordered list of assigned clips (optionally
    species-filtered) is loaded once and cached, and `expert_clip_index`
    tracks where the user currently is. "Previous"/"Next" simply move that
    index — landing on an already-submitted clip re-shows it (pre-filled,
    editable); landing on a fresh one shows a blank form.
    """
    initialize_pro_session()

    # Include dataset_path, user_id, species_filter and language_code in params
    species_filter = selections.get("species_filter")
    species_filter_key = tuple(species_filter) if species_filter else None
    current_params = (
        selections["dataset_path"],
        selections["user_id"],
        species_filter_key,
        selections.get("language_code", "Scientific_Name"),
    )

    # If parameters changed, clear and reload
    if check_params_changed("expert_clip_params", current_params):
        clear_pro_clip_state()
        st.session_state.expert_clip_params = current_params

    # Load the full clip list once per params combination
    if st.session_state.expert_full_clip_list is None:
        from database.queries import (
            filter_clips_by_species,
            get_assigned_clips_for_user,
            get_first_unannotated_index,
            get_user_validation_records,
        )

        all_clips = get_assigned_clips_for_user(
            selections["user_id"], selections["dataset_path"]
        )
        filtered_clips = filter_clips_by_species(all_clips, species_filter)
        st.session_state.expert_full_clip_list = filtered_clips

        validation_records = get_user_validation_records(
            selections["user_id"], selections["dataset_path"]
        )
        st.session_state.expert_clip_index = get_first_unannotated_index(
            filtered_clips, set(validation_records.keys())
        )

    clips = st.session_state.expert_full_clip_list
    index = st.session_state.expert_clip_index

    if not clips or index >= len(clips):
        return {"all_validated": True, "total_clips": len(clips)}

    return clips[index]
