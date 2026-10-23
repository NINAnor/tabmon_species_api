"""Expert Mode UI Components."""

import os

import streamlit as st

from ui.ui_utils import (
    render_all_validated_message,
    render_audio_player,
    render_clip_metadata,
    render_spectrogram,
)


def render_pro_page_header():
    """Render the Expert mode page header."""
    st.title("🎓 TABMON Listening Lab - for Experts!", text_alignment="center")
    st.markdown(
        "Welcome to the Expert annotation mode. "
        "You have been assigned specific clips "
        "to annotate with detailed species identification. "
        "Please carefully listen to each clip and select "
        "all species you can identify."
        "**Note that the app initialization may take a minute or two.**",
        text_alignment="center",
    )


def render_pro_help_section():
    """Render Expert mode help information."""
    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""### 📖 How to use this annotation platform

**Annotation Process:**
1. **Login** with your assigned User ID
2. **Listen** to the assigned audio clip
3. **Select all species** you can identify from the checklist
4. **Add additional species** not in the list if needed
5. **Rate your confidence** and audio quality
6. **Submit** your annotations

**Tips for Better Annotations:**
- 🎧 Use good quality headphones
- 🔊 Listen to the entire clip, sometimes multiple times
- 🔍 Check the spectrogram for visual confirmation
- 📝 Add notes about uncertainties or background noise
""")

        st.markdown("""### 🎯 Quality Guidelines

- **High Confidence:** Clear vocalization, easily identifiable
- **Moderate Confidence:** Recognizable but with some uncertainty
- **Low Confidence:** Difficult to identify, background noise, or distant calls
""")

    with st.expander("⌨️ Keyboard Shortcuts", expanded=False):
        st.markdown("""
- **Ctrl+1** — Toggle the top (highest-confidence) species
- **Ctrl+2** — Toggle the second species
- **Ctrl+3** — Toggle "No bird species audible"
- **Ctrl+4** — Toggle "Unidentified bird present"
- **Ctrl+Enter** — Submit the validation
- **Ctrl+Right** — Next clip
""")


def render_pro_clip_section(result, selections):
    """
    Render the Expert mode audio clip section.

    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections

    Returns:
        bool: True if clip was loaded successfully, False otherwise
    """
    from utils import extract_clip

    if not result:
        st.warning(f"No clips assigned for user {selections['user_id']}")
        return False

    if result.get("all_validated") or not result.get("filename"):
        if not result.get("all_validated"):
            # Clip has no usable file path — skip forward to the next one.
            clips = st.session_state.get("expert_full_clip_list") or []
            index = st.session_state.get("expert_clip_index", 0)
            if clips and index < len(clips) - 1:
                st.warning("Clip has no file path and cannot be displayed. Skipping...")
                st.session_state.expert_clip_index = index + 1
                st.session_state.expert_form_key = (
                    st.session_state.get("expert_form_key", 0) + 1
                )
                st.rerun()
        render_all_validated_message(
            mode_name="assigned clips",
            total_clips=result.get("total_clips", 0),
            extra_message=(
                "Your annotation work is complete. Thank you for your contribution!"
            ),
        )
        return False

    with st.container(border=True):
        st.markdown("### 🎵 Audio Clip")

        filepath = result["filename"].replace("bugg_RpiID", "bugg_RPiID")
        full_path = f"s3://{os.getenv('S3_BUCKET')}/{filepath}"

        context_seconds = st.slider(
            "Context around detection (s)",
            min_value=1,
            max_value=5,
            value=1,
            step=1,
            key="context_seconds",
            help="Seconds of audio context before and after the 3-second BirdNET detection window.",
        )

        clip = extract_clip(full_path, result["start_time"], context_seconds=context_seconds)

        render_clip_metadata(result)
        render_audio_player(clip)
        render_spectrogram(full_path, result["start_time"], expanded=True, context_seconds=context_seconds)

        render_pro_navigation_buttons(selections)

    # Prefetch the next clip's audio and spectrogram into cache
    _prefetch_next_clip_audio()

    return True


def _prefetch_next_clip_audio():
    """Prefetch the next clip's audio and spectrogram into cache."""
    from ui.ui_utils import _generate_spectrogram_image
    from utils import extract_clip

    clips = st.session_state.get("expert_full_clip_list") or []
    index = st.session_state.get("expert_clip_index", 0)
    if clips and index + 1 < len(clips):
        next_clip = clips[index + 1]
        if next_clip.get("filename"):
            filepath = next_clip["filename"].replace("bugg_RpiID", "bugg_RPiID")
            full_path = f"s3://{os.getenv('S3_BUCKET')}/{filepath}"
            context_seconds = st.session_state.get("context_seconds", 1)
            # Warm caches for the next clip
            extract_clip(full_path, next_clip["start_time"], context_seconds=context_seconds)
            _generate_spectrogram_image(full_path, next_clip["start_time"], context_seconds=context_seconds)


def render_pro_navigation_buttons(selections):
    """Render Previous/Next buttons for true position-based navigation
    through the user's full assigned-clip list.

    Both buttons simply move the position index by one; landing on an
    already-submitted clip pre-fills the form (handled in
    validation_handlers.py), landing on a fresh/skipped one shows it blank.
    """
    from database.queries import (
        get_remaining_pro_clips_count,
        get_user_validation_records,
    )

    clips = st.session_state.get("expert_full_clip_list") or []
    index = st.session_state.get("expert_clip_index", 0)
    total = len(clips)

    col_prev, col_next = st.columns(2)

    with col_prev:
        if st.button(
            "⬅️ Previous",
            help="Go back one clip in your assigned list.",
            disabled=(index <= 0),
        ):
            st.session_state.expert_clip_index = max(index - 1, 0)
            st.session_state.expert_form_key = (
                st.session_state.get("expert_form_key", 0) + 1
            )
            st.rerun()

    with col_next:
        if st.button(
            "▶️ Next",
            help="Go to the next clip in your assigned list.",
            shortcut="Ctrl+Right",
            disabled=(index >= total - 1),
        ):
            st.session_state.expert_clip_index = min(index + 1, max(total - 1, 0))
            st.session_state.expert_form_key = (
                st.session_state.get("expert_form_key", 0) + 1
            )
            st.rerun()

    # If we've reached the end of the list but earlier clips are still
    # unannotated (e.g. you navigated past them without submitting), make
    # that visible and offer a quick way back to the first one.
    if clips and index >= total - 1:
        validation_records = get_user_validation_records(
            selections["user_id"], selections["dataset_path"]
        )
        remaining = get_remaining_pro_clips_count(
            clips, set(validation_records.keys())
        )
        if remaining > 0:
            st.warning(
                f"You've reached the end of your list, but **{remaining}** "
                "earlier clip(s) are still unannotated."
            )
            if st.button("⏮ Jump to First Unannotated Clip"):
                from database.queries import get_first_unannotated_index

                st.session_state.expert_clip_index = get_first_unannotated_index(
                    clips, set(validation_records.keys())
                )
                st.session_state.expert_form_key = (
                    st.session_state.get("expert_form_key", 0) + 1
                )
                st.rerun()


def render_pro_empty_validation_placeholder():
    """Render placeholder when no Expert validation form should be shown."""
    with st.container(border=True):
        st.markdown("### 🎯 Expert Validation")
        st.info("Please login with your User ID to access your assigned clips.")


def render_pro_all_validated_placeholder():
    """Render message when all Expert clips have been validated."""
    with st.container(border=True):
        st.markdown("### 🎯 Expert Validation")
        st.success("🎉 All assigned clips have been annotated! Great work!")
