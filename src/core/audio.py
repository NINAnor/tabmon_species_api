"""Audio clip extraction, playback and spectrograms shared by both apps."""

import io
import re
import tempfile
import wave
from datetime import datetime, timedelta
from pathlib import Path

import librosa
import matplotlib.pyplot as plt
import numpy as np
import streamlit as st

from config import AUDIO_GAIN, DETECTION_SECONDS, SAMPLE_RATE
from core.storage import get_s3_client, split_s3_url

SPECTROGRAM_CAPTION = (
    "🔴 Red lines mark the 3-second BirdNET detection. "
    "Focus your validation there — the surrounding audio is for context only."
)


def format_detection_time(filename, start_time):
    """Detection datetime from a file name like 2025-02-09T01_54_51.065Z
    plus the detection offset, or None if the name has no timestamp."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})T(\d{2}_\d{2}_\d{2})", filename)
    if not match or start_time is None:
        return None
    recording_dt = f"{match.group(1)} {match.group(2).replace('_', ':')}"
    try:
        base_dt = datetime.strptime(recording_dt, "%Y-%m-%d %H:%M:%S")
        detection_dt = base_dt + timedelta(seconds=float(start_time))
        return detection_dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return recording_dt


@st.cache_data(ttl=600, show_spinner=False)
def extract_clip(audio_url, start_time, context_seconds=1):
    """Extract a detection clip from an audio file on S3.

    Returns `context_seconds` of audio before and after the 3-second BirdNET
    detection window (clamped to the file boundaries).
    """
    if not audio_url:
        st.error("Could not find audio file in S3")
        return None

    bucket, key = split_s3_url(audio_url)

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
        try:
            get_s3_client().download_file(bucket, key, temp_file.name)
            audio_data, _ = librosa.load(temp_file.name, sr=SAMPLE_RATE, mono=True)
        finally:
            Path(temp_file.name).unlink()

    start_sample = int(max(0, (start_time - context_seconds) * SAMPLE_RATE))
    end_sample = int(
        min(
            len(audio_data),
            (start_time + DETECTION_SECONDS + context_seconds) * SAMPLE_RATE,
        )
    )
    return audio_data[start_sample:end_sample]


@st.cache_data(show_spinner=False)
def generate_spectrogram(audio_url, start_time, context_seconds=1):
    """Generate the clip spectrogram as PNG bytes, with the detection window
    marked by red lines."""
    clip = extract_clip(audio_url, start_time, context_seconds)
    if clip is None:
        return None

    fig, ax = plt.subplots(figsize=(10, 4))
    _, _, _, image = ax.specgram(
        clip,
        Fs=SAMPLE_RATE,
        NFFT=1024,
        noverlap=512,
        cmap="viridis",
        vmin=-120,
    )
    ax.set_ylabel("Frequency (Hz)")
    ax.set_xlabel("Time (s)")
    ax.set_ylim(0, 12000)

    # Both bounds are clamped to the extracted clip duration, which may be
    # shorter than nominal when the detection is near the start or end of the file.
    clip_duration = len(clip) / SAMPLE_RATE
    detection_start = min(context_seconds, start_time, clip_duration)
    detection_end = min(detection_start + DETECTION_SECONDS, clip_duration)
    for x in (detection_start, detection_end):
        ax.axvline(x=x, color="red", linestyle="--", linewidth=1.5, alpha=0.8)

    plt.colorbar(image, ax=ax, label="Intensity (dB)")
    plt.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=100)
    plt.close(fig)
    return buffer.getvalue()


def render_spectrogram(
    audio_url, start_time, context_seconds=1, caption=SPECTROGRAM_CAPTION
):
    """Render the clip spectrogram with a caption."""
    image_bytes = generate_spectrogram(audio_url, start_time, context_seconds)
    if image_bytes:
        st.image(image_bytes, use_container_width=True)
        st.caption(caption)
    else:
        st.warning("Could not generate spectrogram")


def render_audio_player(clip):
    """Render an autoplaying audio player with a fixed gain.

    Encodes audio as WAV bytes to bypass Streamlit's internal full-scale
    normalization (which only triggers on numpy array input).
    """
    if clip is None:
        return

    pcm = np.clip(clip * 32767 * AUDIO_GAIN, -32768, 32767).astype(np.int16)

    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)  # 2 bytes = int16
        wav_file.setframerate(SAMPLE_RATE)
        wav_file.writeframes(pcm.tobytes())

    st.audio(buffer.getvalue(), format="audio/wav", autoplay=True)
