"""
Shared UI Components

Common UI elements used by both normal and pro modes.
"""

from pathlib import Path
import matplotlib.pyplot as plt
import streamlit as st


def setup_page_config():
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="TABMON Listening Lab",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🐦",
    )


def render_sidebar_logo():
    """Render the TABMON logo in the sidebar."""
    logo_path = Path("/app/assets/tabmon_logo.png")
    if logo_path.exists():
        st.sidebar.image(logo_path, width=300)
        st.sidebar.markdown("---")


def render_all_validated_message(mode_name, total_clips, extra_message=""):
    """
    Render success message when all clips are validated.
    
    Args:
        mode_name: Name to display (e.g., "clips", "assigned clips")
        total_clips: Total number of clips completed
        extra_message: Additional message to display
    """
    st.success(f"🎉 Congratulations! All {total_clips} {mode_name} have been validated!")
    st.info(f"✅ {extra_message}" if extra_message else "✅ All validations complete!")
    st.balloons()


@st.cache_data(show_spinner=False)
def _generate_spectrogram_figure(clip_tuple):
    """Generate spectrogram figure (cached).
    Args:
        clip_tuple: Tuple of audio clip (converted from numpy array for hashing)
    """
    import numpy as np
    clip = np.array(clip_tuple)
    
    fig, ax = plt.subplots(figsize=(10, 4))
    
    Pxx, freqs, bins, im = ax.specgram(
        clip,
        Fs=48000,
        NFFT=1024,
        noverlap=512,
        cmap="viridis",
        vmin=-120,
    )
    ax.set_ylabel("Frequency (Hz)")
    ax.set_xlabel("Time (s)")
    ax.set_ylim(0, 12000)
    plt.colorbar(im, ax=ax, label="Intensity (dB)")
    return fig


def render_spectrogram(clip, expanded=False):
    """
    Render audio spectrogram.
    
    Args:
        clip: Audio clip data
        expanded: Whether expander should be open by default
    """
    with st.expander("📊 Spectrogram", expanded=expanded):
        # Convert numpy array to tuple for caching
        clip_tuple = tuple(clip.tolist())
        fig = _generate_spectrogram_figure(clip_tuple)
        st.pyplot(fig)
        plt.close()


def render_audio_player(clip):
    """Render audio player widget."""
    st.audio(clip, format="audio/wav", sample_rate=48000)


def render_clip_metadata(filename):
    """
    Render clip metadata in columns.
    
    Args:
        filename: Audio filename
        confidence: BirdNET confidence score (optional, can be None)
        extra_info: Dict with additional info to display
    """

    st.markdown(f"**📁 File:** `{filename}`")
    

def clear_cache_functions(*functions):
    """Clear cache for multiple functions."""
    for func in functions:
        if hasattr(func, 'clear'):
            func.clear()
