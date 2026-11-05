"""Citizen Mode Session Management."""

import streamlit as st

from citizen.queries import get_audio_url, get_random_detection_clip
from config import CITIZEN_CONTEXT_SECONDS
from core.audio import extract_clip, generate_spectrogram
from core.session import check_params_changed, init_base_session, init_state_vars


def initialize_session():
    """Initialize Citizen mode session state variables."""
    init_base_session()
    init_state_vars(
        "",
        {
            "current_clip": None,
            "next_clip": None,
            "clip_params": None,
            "validation_form_key": 0,
            "session_validation_count": 0,
        },
    )


def get_or_load_clip(selections):
    """Get the current clip, or pick a new random one when the selections
    changed or the previous clip was validated."""
    current_params = (
        selections["country"],
        selections["device"],
        selections["species"],
        selections["confidence_threshold"],
        selections["start_datetime"],
        selections["end_datetime"],
    )

    if check_params_changed("clip_params", current_params):
        st.session_state.clip_params = current_params
        st.session_state.current_clip = None
        st.session_state.next_clip = None

    if st.session_state.current_clip is None:
        st.session_state.current_clip = (
            st.session_state.next_clip or get_random_detection_clip(selections)
        )
        st.session_state.next_clip = None

    return st.session_state.current_clip


def prefetch_next_clip(selections):
    """Pick the next random clip in advance and warm its audio and
    spectrogram caches, so it shows up instantly after submitting."""
    if st.session_state.next_clip is not None:
        return

    current_clip = st.session_state.current_clip or {}
    next_clip = get_random_detection_clip(selections, exclude=current_clip)
    if not next_clip or next_clip.get("all_validated"):
        return

    audio_url = get_audio_url(next_clip["filename"], selections["country"])
    context_seconds = st.session_state.get("context_seconds", CITIZEN_CONTEXT_SECONDS)
    extract_clip(audio_url, next_clip["start_time"], context_seconds)
    generate_spectrogram(audio_url, next_clip["start_time"], context_seconds)
    st.session_state.next_clip = next_clip


def load_next_clip():
    """Drop the current clip and reset the form so a new clip is shown."""
    st.session_state.current_clip = None
    st.session_state.validation_form_key += 1
