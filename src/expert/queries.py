"""Expert mode data access: assigned datasets and expert validation records."""

import ast

import pandas as pd
import streamlit as st

from config import EXPERT_DATASETS_PREFIX, EXPERT_VALIDATIONS_PREFIX
from core.storage import (
    append_csv,
    get_duckdb_connection,
    list_objects,
    read_csv_files,
    s3_url,
)


def get_dataset_name(dataset_path):
    """Dataset name from its S3 path, e.g. '.../birds_2025.parquet' -> 'birds_2025'."""
    if not dataset_path:
        return ""
    return dataset_path.split("/")[-1].removesuffix(".parquet")


def get_audio_url(filename):
    """S3 URL of a clip's audio file (`fullPath` column of the dataset)."""
    return s3_url(filename.replace("bugg_RpiID", "bugg_RPiID"))


def clip_key(clip):
    """Key identifying a clip in validation records."""
    return (clip["filename"], clip["start_time"])


def parse_list_cell(value):
    """Parse a CSV cell that may contain a list representation.

    Cells written by `save_validation` hold Python-list reprs
    (e.g. "['Other animal', 'Bird']"). Older rows contain a single plain
    value or 'nan'. Always returns a list of strings.
    """
    text = "" if value is None else str(value)
    text = "" if text.lower() == "nan" else text.strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list | tuple):
                return [str(item) for item in parsed]
        except (ValueError, SyntaxError):
            return [text]
    return [text]


# ============================================================================
# Assigned datasets
# ============================================================================


def list_available_datasets():
    """List all parquet datasets in the validation_dataset folder."""
    try:
        datasets = [
            {
                "name": get_dataset_name(obj["Key"]),
                "path": s3_url(obj["Key"]),
                "size": obj["Size"],
            }
            for obj in list_objects(EXPERT_DATASETS_PREFIX)
            if obj["Key"].endswith(".parquet")
        ]
        return sorted(datasets, key=lambda dataset: dataset["name"])
    except Exception as e:
        st.error(f"Error listing datasets: {e}")
        return []


def check_user_has_annotations(user_id, dataset_path):
    """Check if user has any assigned annotations in the selected dataset."""
    if not dataset_path:
        return False
    try:
        result = (
            get_duckdb_connection()
            .execute(
                f"SELECT COUNT(*) FROM '{dataset_path}' "
                "WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)",
                [user_id],
            )
            .fetchone()
        )
        return result[0] > 0 if result else False
    except Exception:
        return False


@st.cache_data(ttl=300)
def get_species_for_user(user_id, dataset_path):
    """Unique species assigned to a user in the dataset, sorted."""
    query = f"""
    SELECT DISTINCT unnest("scientific name") as species
    FROM '{dataset_path}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    ORDER BY species
    """
    try:
        results = get_duckdb_connection().execute(query, [user_id]).fetchall()
        return [row[0] for row in results]
    except Exception:
        return []


@st.cache_data(ttl=1800, show_spinner="Loading assigned clips...")
def get_assigned_clips_for_user(user_id, dataset_path):
    """All clips assigned to a user in the dataset, as a list of dicts."""
    query = f"""
    SELECT
        fullPath as filename,
        deployment_id,
        "start time" as start_time,
        "scientific name" as species_array,
        confidence as confidence_array,
        userID
    FROM '{dataset_path}'
    WHERE CAST(userID AS VARCHAR) = CAST(? AS VARCHAR)
    ORDER BY filename, start_time
    """
    cursor = get_duckdb_connection().execute(query, [user_id])
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def filter_clips_by_species(clips, species_filter):
    """Keep clips whose detected species include any of `species_filter`."""
    if not species_filter:
        return clips
    species_set = set(species_filter)
    return [
        clip for clip in clips if set(clip.get("species_array") or []) & species_set
    ]


def get_remaining_clips_count(clips, validated_keys):
    """Count how many clips in the list are not yet validated."""
    return sum(1 for clip in clips if clip_key(clip) not in validated_keys)


def get_first_unannotated_index(clips, validated_keys):
    """Index of the first not-yet-validated clip, or len(clips) if all are done."""
    for index, clip in enumerate(clips):
        if clip_key(clip) not in validated_keys:
            return index
    return len(clips)


# ============================================================================
# Validation records
# ============================================================================


def read_dataset_validations(dataset_path):
    """Raw validation rows of every annotator for a dataset."""
    return read_csv_files(
        EXPERT_VALIDATIONS_PREFIX, f"{get_dataset_name(dataset_path)}_"
    )


@st.cache_data(ttl=300)
def get_user_validation_records(user_id, dataset_path):
    """Load this user's own submitted validation rows for the dataset.

    Returns {(filename, start_time): row_dict} keeping only the most recent
    row (by timestamp) per clip, so prior corrections (is_update=True rows)
    are reflected rather than the original mistaken submission.
    """
    try:
        validations = read_dataset_validations(dataset_path)
        if validations.empty:
            return {}

        user_rows = validations[
            validations["userID"].astype(str) == str(user_id)
        ].copy()
        user_rows["timestamp"] = pd.to_datetime(user_rows["timestamp"], errors="coerce")
        user_rows = user_rows.sort_values("timestamp")

        return {clip_key(row): row.to_dict() for _, row in user_rows.iterrows()}
    except Exception:
        return {}


def _pipe_join(value):
    """Join a list into a pipe-separated string for CSV storage."""
    return "|".join(str(item) for item in value) if isinstance(value, list) else value


def _list_repr(value):
    """Serialize a multi-value column as a list cell, e.g. "['Other animal',
    'Bird']". pandas quotes the cell; `parse_list_cell` reads it back."""
    return str(value) if isinstance(value, list) else value


def save_validation(validation_data):
    """Append an expert validation to this session's CSV file on S3."""
    dataset_name = (
        get_dataset_name(st.session_state.get("expert_selected_dataset")) or "unknown"
    )
    key = (
        f"{EXPERT_VALIDATIONS_PREFIX}/"
        f"{dataset_name}_session_{st.session_state.session_id}.csv"
    )

    pipe_joined_columns = [
        "identified_species",
        "birdnet_species_detected",
        "birdnet_confidences",
        "user_notes",
        "fp_target_species",
        "fp_target_species_confidence",
        "fp_likely_mechanism",
    ]
    row = {
        **validation_data,
        **{
            column: _pipe_join(validation_data.get(column))
            for column in pipe_joined_columns
        },
        "vocalization_types": "|".join(
            f"{species}:{vocalization}"
            for species, vocalization in (
                validation_data.get("vocalization_types") or {}
            ).items()
        ),
        "is_cross_validation": validation_data.get("is_cross_validation", False),
        "is_update": validation_data.get("is_update", False),
        "is_fp_assessment": validation_data.get("is_fp_assessment", False),
        "fp_source_category": _list_repr(validation_data.get("fp_source_category")),
        "fp_source_detail": _list_repr(validation_data.get("fp_source_detail")),
    }

    try:
        append_csv(pd.DataFrame([row]), key)
    except Exception as e:
        st.error(f"Error saving Expert validation: {e}")
        return False

    # Pick up the just-saved row (and updated remaining count) on next render.
    get_user_validation_records.clear()
    return True
