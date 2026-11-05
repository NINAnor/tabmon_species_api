"""Species name translations shared by both apps."""

import pandas as pd
import streamlit as st

from config import BIRDNET_MULTILINGUAL_PATH

SCIENTIFIC_NAME = "Scientific_Name"


@st.cache_data
def load_species_translations():
    """Load the multilingual species name translations."""
    return pd.read_csv(BIRDNET_MULTILINGUAL_PATH)


def get_species_display_names(species_list, language_code):
    """Map display names in the selected language to scientific names.

    Returns {display_name: scientific_name}, falling back to the scientific
    name when no translation exists.
    """
    if language_code == SCIENTIFIC_NAME:
        return {species: species for species in species_list}

    translations_df = load_species_translations()
    species_map = {}

    for species in species_list:
        translation_row = translations_df[translations_df[SCIENTIFIC_NAME] == species]
        translated_name = species

        if not translation_row.empty and language_code in translation_row.columns:
            translation = translation_row[language_code].iloc[0]
            if pd.notna(translation):
                translated_name = translation

        species_map[translated_name] = species

    return species_map


@st.cache_data
def get_all_species_list(language_code=SCIENTIFIC_NAME):
    """All species names for autocomplete, as "Common Name (Scientific Name)"
    in the selected language."""
    translations_df = load_species_translations()

    if language_code == SCIENTIFIC_NAME or language_code not in translations_df.columns:
        return sorted(translations_df[SCIENTIFIC_NAME].dropna().tolist())

    species_list = [
        f"{row[language_code]} ({row[SCIENTIFIC_NAME]})"
        if pd.notna(row.get(language_code))
        else row[SCIENTIFIC_NAME]
        for _, row in translations_df.iterrows()
    ]
    return sorted(species_list)


def extract_scientific(display_name):
    """Parse "Common Name (Scientific Name)" back to the scientific name.

    Other strings (e.g. free text like 'lark, sparrow') pass through unchanged.
    """
    if not display_name or " (" not in display_name or not display_name.endswith(")"):
        return display_name
    return display_name.split(" (")[-1].rstrip(")")
