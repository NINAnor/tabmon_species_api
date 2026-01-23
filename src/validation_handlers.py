"""
Pro Mode Validation Handlers

This module handles the validation form for Pro mode, which uses
a checklist of the top 10 species instead of a simple Yes/No response.
"""

import ast
import pandas as pd
import streamlit as st

from queries import get_remaining_pro_clips_count, get_top_species_for_database
from utils import get_species_display_names


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
    Render the Pro mode validation form with species checklist.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
    """
    with st.container(border=True):
        st.markdown("### 🎯 Pro Validation")

        # Show remaining clips count from session state (updated on validation)
        if 'pro_remaining_count' in st.session_state and st.session_state.pro_remaining_count is not None:
            remaining_clips = st.session_state.pro_remaining_count
            if remaining_clips > 0:
                st.info(
                    f"📊 Still **{remaining_clips}** clips to annotate"
                )
            else:
                st.success("🎉 All clips validated!")

        with st.form("pro_validation_form"):
            st.markdown(f"#### Species detected by BirdNET in this clip:")
            st.markdown("**Select which species you can actually hear:**")
            st.markdown("---")
            
            # Get the detected species arrays
            species_list = _parse_array_string(result.get('species_array', []))
            confidence_list = _parse_array_string(result.get('confidence_array', []))
            uncertainty_list = _parse_array_string(result.get('uncertainty_array', []))
            
            # Create list of tuples (species, confidence, uncertainty) and sort by confidence (descending)
            species_data = []
            for idx, species in enumerate(species_list):
                confidence = confidence_list[idx] if idx < len(confidence_list) else 0.0
                uncertainty = uncertainty_list[idx] if idx < len(uncertainty_list) else 0.0
                try:
                    conf_val = float(confidence)
                    uncert_val = float(uncertainty)
                except (ValueError, TypeError):
                    conf_val = 0.0
                    uncert_val = 0.0
                species_data.append((species, conf_val, uncert_val))
            
            # Sort by confidence (highest first)
            species_data.sort(key=lambda x: x[1], reverse=True)
            
            # Get language code from selections
            language_code = selections.get("language_code", "Scientific_Name")
            
            # Get translation mapping for detected species (display_name -> scientific_name)
            scientific_names = [species for species, _, _ in species_data]
            display_name_map = get_species_display_names(scientific_names, language_code)
            
            # Create reverse mapping (scientific_name -> display_name) for checkbox labels
            scientific_to_display = {sci_name: disp_name for disp_name, sci_name in display_name_map.items()}
            
            # Create checklist for detected species
            selected_species = []
            
            # Display each detected species with its confidence (using translated names)
            for idx, (species, conf_val, uncert_val) in enumerate(species_data):
                display_name = scientific_to_display.get(species, species)
                label = f"{display_name} (Birdnet conf: {conf_val:.2f})"
                
                if st.checkbox(label, key=f"species_{idx}"):
                    # Store scientific name, not display name
                    selected_species.append(species)
            
            none_of_above = st.checkbox(
                "❌ None of the above species are present",
                key="none_of_above",
                help="Check this if you cannot hear any of the species listed above"
            )
            
            # If "None of the above" is checked, clear selected species and add marker
            if none_of_above:
                selected_species = ["NONE_DETECTED"]
            
            st.markdown("---")
            
            # Additional species input for species not in top 10
            other_species = st.text_input(
                "**Other species not listed above:**",
                placeholder="e.g., Parus major, Sturnus vulgaris",
                help="Enter additional species names separated by commas"
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
                    other_species,
                    user_notes,
                    user_confidence
                )


def _handle_pro_validation_submission(
    result, selections, selected_species, other_species, 
    user_notes, user_confidence
):
    """
    Handle Pro mode validation form submission.
    
    Args:
        result: Dictionary containing clip information
        selections: Dictionary containing user selections
        selected_species: List of selected species from checklist
        other_species: String of additional species names
        user_notes: User's notes/comments
        user_confidence: User's confidence level
    """
    # Validation: require at least confidence rating
    if not user_confidence:
        st.error("Please rate your confidence before submitting.")
        return
    
    # Parse other species if provided
    additional_species = []
    if other_species:
        additional_species = [s.strip() for s in other_species.split(",") if s.strip()]
    
    # Combine all identified species
    all_identified_species = selected_species + additional_species
    
    # Prepare validation data
    validation_data = {
        "filename": result["filename"],
        "userID": selections["user_id"],
        "deployment_id": result.get("deployment_id", ""),
        "birdnet_species_detected": result.get("species_array", []),  # Array of what BirdNET detected
        "birdnet_confidences": result.get("confidence_array", []),  # Array of confidences
        "birdnet_uncertainties": result.get("uncertainty_array", []),  # Array of uncertainties
        "start_time": result["start_time"],
        "identified_species": all_identified_species,  # List of species user confirmed/added
        "species_count": len(all_identified_species),  # Number of species identified
        "user_confidence": user_confidence,
        "user_notes": user_notes,
        "timestamp": pd.Timestamp.now(),
    }
    
    # Save validation response
    from utils import save_pro_validation_response
    save_pro_validation_response(validation_data)
    
    # Clear current clip to force loading a new one
    st.session_state.pro_current_clip = None
    
    st.success("✅ Thank you for your annotation!")
    st.rerun()
