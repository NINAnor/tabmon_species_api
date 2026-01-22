"""
Pro Mode Selection Handlers

This module manages user input selections for Pro mode.
"""

import streamlit as st

from pro.queries import check_user_has_annotations
from shared.ui_utils import render_sidebar_logo


def render_pro_authentication():
    """
    Render authentication form for Pro mode.
    Allows any user to enter their ID and access their assigned annotations.
    
    Returns:
        tuple: (is_authenticated, user_id) or (False, None)
    """
    render_sidebar_logo()
    
    st.sidebar.header("🔐 Pro Mode Authentication")
    
    # Check if already authenticated
    if "pro_authenticated" in st.session_state and st.session_state.pro_authenticated:
        user_id = st.session_state.pro_user_id
        st.sidebar.success(f"✅ Authenticated as: **{user_id}**")
        
        if st.sidebar.button("🚪 Logout"):
            st.session_state.pro_authenticated = False
            st.session_state.pro_user_id = None
            st.rerun()
        
        return True, user_id
    
    # User ID input (free text field)
    user_id = st.sidebar.text_input(
        "Enter Your User ID",
        help="Enter the user ID assigned to you for annotation tasks",
        placeholder="e.g., user001"
    )
    
    # Login button
    if st.sidebar.button("🔓 Login", type="primary"):
        if not user_id or not user_id.strip():
            st.sidebar.error("❌ Please enter a user ID.")
            return False, None
        
        user_id = user_id.strip()
        
        # Check if user has any assigned annotations
        if check_user_has_annotations(user_id):
            st.session_state.pro_authenticated = True
            st.session_state.pro_user_id = user_id
            st.rerun()
        else:
            st.sidebar.error(f"❌ No annotations found for user ID: {user_id}")
            return False, None
    
    st.sidebar.info("Please enter your user ID to continue")
    return False, None


def get_pro_user_selections():
    """
    Get user selections for Pro mode.
    
    Returns:
        Dictionary with user_id and confidence_threshold, or None if not authenticated
    """
    # Authentication check
    is_authenticated, user_id = render_pro_authentication()
    
    if not is_authenticated:
        return None
    
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 Parameters")
    
    # Confidence threshold (same as normal mode)
    confidence_threshold = st.sidebar.slider(
        "Minimum Confidence Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.1,
        help="Only show clips with BirdNET confidence above this threshold",
    )
    
    # Show user info
    st.sidebar.markdown("---")
    st.sidebar.info(f"👤 **User:** {user_id}")
    
    return {
        "user_id": user_id,
        "confidence_threshold": confidence_threshold,
    }
