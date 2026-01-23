"""Expert Mode Validation Handlers - checklist of top species."""

import ast
import pandas as pd
import streamlit as st

from database.queries import get_remaining_pro_clips_count, get_top_species_for_database
from utils import get_species_display_names, load_species_translations


@st.cache_data
def _get_all_species_list(language_code="Scientific_Name"):
    """Get species names for autocomplete in selected language."""
    translations_df = load_species_translations()
    
    if language_code == "Scientific_Name" or language_code not in translations_df.columns:
        return sorted(translations_df["Scientific_Name"].dropna().tolist())
    
    species_list = [
        f"{row[language_code]} ({row['Scientific_Name']})" if pd.notna(row.get(language_code))
        else row["Scientific_Name"]
        for _, row in translations_df.iterrows()
    ]
    return sorted(species_list)


@st.cache_data(show_spinner=False)
def _parse_array_string(array_str):
    """Parse array string from parquet, handling Unicode quotes.
    Cached to avoid repeated parsing of same data."""
    if not isinstance(array_str, str):
        return array_str
    
    # Replace Unicode curly quotes with ASCII quotes
    cleaned = array_str.replace('\u201c', '"').replace('\u201d', '"').replace('\u2018', "'").replace('\u2019', "'")
    return ast.literal_eval(cleaned)


def render_pro_validation_form(result, selections):
    """
    Render the Expert mode validation form with species checklist.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Expert Validation")

        # Show remaining clips count from session state
        if 'expert_remaining_count' in st.session_state and st.session_state.expert_remaining_count is not None:
            remaining = st.session_state.expert_remaining_count
            st.success("🎉 All clips validated!") if remaining == 0 else st.info(f"📊 Still **{remaining}** clips to annotate")

        with st.form("expert_validation_form"):
            st.markdown("#### Species detected by BirdNET in this clip:")
            st.markdown("**Select which species you can actually hear:**")
            st.markdown("---")
            
            # Parse and sort species by confidence (descending)
            species_list = _parse_array_string(result.get('species_array', []))
            confidence_list = _parse_array_string(result.get('confidence_array', []))
            uncertainty_list = _parse_array_string(result.get('uncertainty_array', []))
            
            species_data = sorted([
                (species, 
                 float(confidence_list[idx]) if idx < len(confidence_list) else 0.0,
                 float(uncertainty_list[idx]) if idx < len(uncertainty_list) else 0.0)
                for idx, species in enumerate(species_list)
            ], key=lambda x: x[1], reverse=True)
            
            # Get translations
            language_code = selections.get("language_code", "Scientific_Name")
            scientific_names = [species for species, _, _ in species_data]
            display_name_map = get_species_display_names(scientific_names, language_code)
            scientific_to_display = {sci: disp for disp, sci in display_name_map.items()}
            
            # Display checklist
            selected_species = []
            for idx, (species, conf_val, _) in enumerate(species_data):
                display_name = scientific_to_display.get(species, species)
                if st.checkbox(f"{display_name} (Birdnet conf: {conf_val:.2f})", key=f"species_{idx}"):
                    selected_species.append(species)
            
            none_of_above = st.checkbox(
                "❌ None of the above species are present",
                key="none_of_above",
                help="Check this if you cannot hear any of the species listed above"
            )
            
            if none_of_above:
                selected_species = ["NONE_DETECTED"]
            
            st.markdown("---")
            
            # Get all available species for autocomplete
            language_code = selections.get("language_code", "Scientific_Name")
            all_species = _get_all_species_list(language_code)
            
            # Additional species input with autocomplete (multiselect)
            other_species_list = st.multiselect(
                "**Other species not listed above:**",
                options=all_species,
                default=[],
                help="Search and select additional species not in the checklist above",
                placeholder="Start typing to search..."
            )
            
            # Notes/comments field
            user_notes = st.text_area(
                "**Additional notes or observations:**",
                placeholder="e.g., background noise, quality issues, uncertainty...",
                help="Optional: Add any relevant notes about this clip"
            )
            
            # Confidence rating
            user_confidence = st.radio(
                "**How confident are you in your annotations?**",
                options=["Low", "Moderate", "High"],
                index=None,
                horizontal=True,
                help="Rate your overall confidence in the species identifications above"
            )

            submitted = st.form_submit_button(
                "✅ Submit Validation", type="primary", use_container_width=True
            )
        
            if submitted:
                _handle_pro_validation_submission(
                    result,
                    selections,
                    selected_species,
                    other_species_list,
                    user_notes,
                    user_confidence
                )


def _handle_pro_validation_submission(result, selections, selected_species, other_species_list, user_notes, user_confidence):
    """Handle Expert mode validation form submission."""
    if not user_confidence:
        st.error("Please rate your confidence before submitting.")
        return
    
    # Extract scientific names from multiselect (format: "Common Name (Scientific Name)")
    additional_species = [
        species_str.split(" (")[-1].rstrip(")") if " (" in species_str and species_str.endswith(")")
        else species_str
        for species_str in other_species_list
    ]
    
    all_identified_species = selected_species + additional_species
    
    # Parse arrays from parquet to ensure they're proper lists
    birdnet_species = _parse_array_string(result.get("species_array", []))
    birdnet_confidences = _parse_array_string(result.get("confidence_array", []))
    birdnet_uncertainties = _parse_array_string(result.get("uncertainty_array", []))
    
    # Prepare validation data
    validation_data = {
        "filename": result["filename"],
        "userID": selections["user_id"],
        "deployment_id": result.get("deployment_id", ""),
        "birdnet_species_detected": birdnet_species,
        "birdnet_confidences": birdnet_confidences,
        "birdnet_uncertainties": birdnet_uncertainties,
        "start_time": result["start_time"],
        "identified_species": all_identified_species,
        "species_count": len(all_identified_species),  # Number of species identified
        "user_confidence": user_confidence,
        "user_notes": user_notes,
        "timestamp": pd.Timestamp.now(),
    }
    
    # Save validation response
    from utils import save_pro_validation_response
    save_pro_validation_response(validation_data)
    
    # Clear current clip to force loading a new one
    st.session_state.expert_current_clip = None
    
    st.success("✅ Thank you for your annotation!")
    st.rerun()
