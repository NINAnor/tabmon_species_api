"""Expert Mode UI Components."""

import streamlit as st

from core.audio import (
    extract_clip,
    format_detection_time,
    generate_spectrogram,
    render_audio_player,
    render_spectrogram,
)
from core.sites import get_site_info_for_deployment
from core.ui import render_context_slider
from expert.queries import (
    get_audio_url,
    get_first_unannotated_index,
    get_remaining_clips_count,
    get_user_validation_records,
)
from expert.session import bump_expert_form_key


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
- **1** — Toggle the top (highest-confidence) species
- **2** — Toggle "None of the listed species are present"
- **3** — Toggle "No bird species audible"
- **4** — Toggle "Unidentified bird present"
- **q / w / e** — Confidence: Low / Moderate / High
- **a** — Previous clip
- **d** — Next clip
- **Space** — Submit the validation
""")


def render_clip_metadata(clip):
    """Render clip metadata: site, cluster and detection time."""
    filename = clip.get("filename", "").split("/")[-1]
    deployment_id = clip.get("deployment_id", "")
    start_time = clip.get("start_time")

    detection_time = format_detection_time(filename, start_time)

    if deployment_id:
        site_info = get_site_info_for_deployment(deployment_id)
        st.markdown(f"**📍 Site:** `{site_info.get('site', deployment_id)}`")
        if site_info.get("cluster"):
            st.markdown(f"**🏘️ Cluster:** `{site_info['cluster']}`")
    if detection_time:
        st.markdown(f"**🔊 Detection time:** `{detection_time}`")


def render_pro_clip_section(result, selections):
    """
    Render the Expert mode audio clip section.

    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections

    Returns:
        bool: True if clip was loaded successfully, False otherwise
    """
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
                bump_expert_form_key()
                st.rerun()
        st.success(
            f"🎉 Congratulations! All {result.get('total_clips', 0)} assigned clips "
            "have been validated!"
        )
        st.info("✅ Your annotation work is complete. Thank you for your contribution!")
        st.balloons()
        return False

    with st.container(border=True):
        st.markdown("### 🎵 Audio Clip")

        audio_url = get_audio_url(result["filename"])
        context_seconds = render_context_slider("context_seconds")
        clip = extract_clip(audio_url, result["start_time"], context_seconds)

        render_clip_metadata(result)
        render_audio_player(clip)
        with st.expander("📊 Spectrogram", expanded=True):
            render_spectrogram(audio_url, result["start_time"], context_seconds)

        render_pro_navigation_buttons(selections)

    _prefetch_next_clip_audio()

    return True


def _prefetch_next_clip_audio():
    """Warm the audio and spectrogram caches for the next clip."""
    clips = st.session_state.get("expert_full_clip_list") or []
    index = st.session_state.get("expert_clip_index", 0)
    if not clips or index + 1 >= len(clips):
        return
    next_clip = clips[index + 1]
    if next_clip.get("filename"):
        audio_url = get_audio_url(next_clip["filename"])
        context_seconds = st.session_state.get("context_seconds", 1)
        extract_clip(audio_url, next_clip["start_time"], context_seconds)
        generate_spectrogram(audio_url, next_clip["start_time"], context_seconds)


def render_pro_navigation_buttons(selections):
    """Render Previous/Next buttons for position-based navigation through
    the user's full assigned-clip list.

    Both buttons simply move the position index by one; landing on an
    already-submitted clip pre-fills the form (handled in validation.py),
    landing on a fresh/skipped one shows it blank.
    """
    clips = st.session_state.get("expert_full_clip_list") or []
    index = st.session_state.get("expert_clip_index", 0)
    total = len(clips)

    col_prev, col_next = st.columns(2)

    with col_prev:
        if st.button(
            "⬅️ Previous",
            help="Go back one clip in your assigned list.",
            shortcut="a",
            disabled=(index <= 0),
        ):
            st.session_state.expert_clip_index = max(index - 1, 0)
            bump_expert_form_key()
            st.rerun()

    with col_next:
        if st.button(
            "▶️ Next",
            help="Go to the next clip in your assigned list.",
            shortcut="d",
            disabled=(index >= total - 1),
        ):
            st.session_state.expert_clip_index = min(index + 1, max(total - 1, 0))
            bump_expert_form_key()
            st.rerun()

    # If we've reached the end of the list but earlier clips are still
    # unannotated (e.g. you navigated past them without submitting), make
    # that visible and offer a quick way back to the first one.
    if clips and index >= total - 1:
        validated_keys = set(
            get_user_validation_records(
                selections["user_id"], selections["dataset_path"]
            )
        )
        remaining = get_remaining_clips_count(clips, validated_keys)
        if remaining > 0:
            st.warning(
                f"You've reached the end of your list, but **{remaining}** "
                "earlier clip(s) are still unannotated."
            )
            if st.button("⏮ Jump to First Unannotated Clip"):
                st.session_state.expert_clip_index = get_first_unannotated_index(
                    clips, validated_keys
                )
                bump_expert_form_key()
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
