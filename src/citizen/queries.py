"""Citizen mode data access: BirdNET predictions, audio lookup and validations."""

import random

import pandas as pd
import streamlit as st

from config import (
    AUDIO_COUNTRY_SUFFIXES,
    AUDIO_S3_PREFIX,
    CITIZEN_VALIDATIONS_PREFIX,
    PREDICTIONS_S3_PREFIX,
)
from core.storage import (
    append_csv,
    get_duckdb_connection,
    list_subfolders,
    object_exists,
    read_csv_files,
    s3_url,
)

# Detection datetime = recording start parsed from the file name
# (e.g. ...2025-02-09T01_54_51...) + the detection offset in seconds.
_FILENAME_DATETIME = r"(\d{4}-\d{2}-\d{2})T(\d{2}_\d{2}_\d{2})"
_DETECTION_DATETIME = f"""
    try_strptime(
        regexp_extract(filename, '{_FILENAME_DATETIME}', 1)
        || ' ' ||
        replace(regexp_extract(filename, '{_FILENAME_DATETIME}', 2), '_', ':'),
        '%Y-%m-%d %H:%M:%S'
    ) + ("start time" * INTERVAL 1 SECOND)
"""


def _predictions_path(country, device_id):
    return s3_url(
        f"{PREDICTIONS_S3_PREFIX}/country={country}/device_id={device_id}/*.parquet"
    )


def _datetime_filter(start_datetime, end_datetime):
    """SQL conditions and parameters for an optional datetime range."""
    conditions = ""
    params = []
    if start_datetime:
        conditions += " AND detection_datetime >= ?"
        params.append(start_datetime)
    if end_datetime:
        conditions += " AND detection_datetime <= ?"
        params.append(end_datetime)
    return conditions, params


# ============================================================================
# Predictions
# ============================================================================


@st.cache_data(ttl=600, show_spinner="Loading species...")
def get_species_for_site(country, device_id, start_datetime=None, end_datetime=None):
    """Species with at least 5 detections at a site, sorted."""
    conditions, params = _datetime_filter(start_datetime, end_datetime)
    query = f"""
    WITH detections AS (
        SELECT "scientific name", {_DETECTION_DATETIME} AS detection_datetime
        FROM '{_predictions_path(country, device_id)}'
    )
    SELECT "scientific name", COUNT(*) AS cnt
    FROM detections
    WHERE 1=1 {conditions}
    GROUP BY "scientific name"
    HAVING cnt >= 5
    ORDER BY "scientific name"
    """
    try:
        return [
            row[0] for row in get_duckdb_connection().execute(query, params).fetchall()
        ]
    except Exception:
        return []


@st.cache_data(ttl=600)
def get_datetime_bounds_for_site(country, device_id):
    """(min, max) detection datetime at a site, or (None, None)."""
    query = f"""
    SELECT MIN(detection_datetime), MAX(detection_datetime)
    FROM (
        SELECT {_DETECTION_DATETIME} AS detection_datetime
        FROM '{_predictions_path(country, device_id)}'
    )
    WHERE detection_datetime IS NOT NULL
    """
    try:
        return get_duckdb_connection().execute(query).fetchone()
    except Exception:
        return (None, None)


@st.cache_data(ttl=3600, show_spinner="Extracting the species at the selected site")
def get_all_clips_for_species(
    country,
    device_id,
    species,
    confidence_threshold=0.0,
    start_datetime=None,
    end_datetime=None,
):
    """All detections of a species at a site above the confidence threshold,
    as a list of {filename, start_time, confidence} dicts."""
    conditions, datetime_params = _datetime_filter(start_datetime, end_datetime)
    query = f"""
    WITH detections AS (
        SELECT
            filename,
            "start time",
            confidence,
            "scientific name",
            {_DETECTION_DATETIME} AS detection_datetime
        FROM '{_predictions_path(country, device_id)}'
    )
    SELECT filename, "start time", confidence
    FROM detections
    WHERE "scientific name" = ? AND confidence >= ? {conditions}
    ORDER BY confidence DESC
    """
    try:
        rows = (
            get_duckdb_connection()
            .execute(query, [species, confidence_threshold, *datetime_params])
            .fetchall()
        )
    except Exception:
        return []
    return [
        {"filename": filename, "start_time": start_time, "confidence": confidence}
        for filename, start_time, confidence in rows
    ]


def get_random_detection_clip(selections, exclude=None):
    """A random not-yet-validated clip for the selections (other than
    `exclude`), or an {"all_validated": True, ...} marker when none is left."""
    all_clips = get_all_clips_for_species(*_clip_query_args(selections))
    if not all_clips:
        return None

    skipped_clips = get_validated_clips(
        selections["country"], selections["device"], selections["species"]
    )
    if exclude:
        skipped_clips = skipped_clips | {(exclude["filename"], exclude["start_time"])}
    unvalidated_clips = [
        clip
        for clip in all_clips
        if (clip["filename"], clip["start_time"]) not in skipped_clips
    ]

    if not unvalidated_clips:
        return {"all_validated": True, "total_clips": len(all_clips)}

    return random.choice(unvalidated_clips)


def get_validation_progress(selections):
    """(validated_count, total_clips) for the selections."""
    total_clips = len(get_all_clips_for_species(*_clip_query_args(selections)))
    validated_count = len(
        get_validated_clips(
            selections["country"], selections["device"], selections["species"]
        )
    )
    return min(validated_count, total_clips), total_clips


def _clip_query_args(selections):
    return (
        selections["country"],
        selections["device"],
        selections["species"],
        selections["confidence_threshold"],
        selections["start_datetime"],
        selections["end_datetime"],
    )


# ============================================================================
# Audio lookup
# ============================================================================


@st.cache_data(ttl=3600, show_spinner=False)
def get_audio_url(filename, country):
    """Find the S3 URL of an audio file by searching the country's
    <device>/conf_*/ folders."""
    country_prefix = f"{AUDIO_S3_PREFIX}{AUDIO_COUNTRY_SUFFIXES.get(country, '')}/"
    try:
        device_folders = list_subfolders(country_prefix)
        if not device_folders:
            st.warning(f"No device directories found under {country_prefix}")
            return None

        for device_folder in device_folders:
            for conf_folder in list_subfolders(device_folder):
                key = f"{conf_folder}{filename}"
                if object_exists(key):
                    return s3_url(key)
    except Exception as e:
        st.error(f"Error searching for file: {e}")
        return None

    st.error(f"Could not find file {filename} in country {country}")
    return None


# ============================================================================
# Validations
# ============================================================================


@st.cache_data(ttl=300)
def get_validated_clips(country, device_id, species):
    """Set of (filename, start_time) already validated by anyone for
    this country/device/species."""
    try:
        validations = read_csv_files(CITIZEN_VALIDATIONS_PREFIX)
        if validations.empty:
            return set()
        matching = validations[
            (validations["country"] == country)
            & (validations["device_id"] == device_id)
            & (validations["species"] == species)
        ]
        return set(zip(matching["filename"], matching["start_time"], strict=True))
    except Exception:
        return set()


def save_validation(validation_data):
    """Append a citizen validation to this session's CSV file on S3."""
    key = f"{CITIZEN_VALIDATIONS_PREFIX}/session_{st.session_state.session_id}.csv"
    try:
        append_csv(pd.DataFrame([validation_data]), key)
    except Exception as e:
        st.error(f"Error saving validation: {e}")
        return False
    get_validated_clips.clear()
    return True
