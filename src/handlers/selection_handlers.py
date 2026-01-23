"""Expert Mode Selection Handlers.
Manages user authentication and language selection.
"""

import streamlit as st

from config import LANGUAGE_MAPPING
from database.queries import check_user_has_annotations
from ui.ui_utils import render_sidebar_logo


def render_pro_authentication():
    """Render authentication form for Expert mode.
    Returns (is_authenticated, user_id).
    """
    render_sidebar_logo()
    st.sidebar.header("🔐 Expert Mode Authentication")

    if (
        "expert_authenticated" in st.session_state
        and st.session_state.expert_authenticated
    ):
        user_id = st.session_state.expert_user_id
        st.sidebar.success(f"✅ Authenticated as: **{user_id}**")

        if st.sidebar.button("🚪 Logout"):
            st.session_state.expert_authenticated = False
            st.session_state.expert_user_id = None
            st.rerun()

        return True, user_id

    user_id = st.sidebar.text_input(
        "Enter Your User ID",
        help="Enter the user ID assigned to you for annotation tasks",
        placeholder="e.g., user001",
    )

    if st.sidebar.button("🔓 Login", type="primary"):
        if not user_id or not user_id.strip():
            st.sidebar.error("❌ Please enter a user ID.")
            return False, None

        user_id = user_id.strip()

        # Check if user has any assigned annotations
        if check_user_has_annotations(user_id):
            st.session_state.expert_authenticated = True
            st.session_state.expert_user_id = user_id
            st.rerun()
        else:
            st.sidebar.error(f"❌ No annotations found for user ID: {user_id}")
            return False, None

    st.sidebar.info("Please enter your user ID to continue")
    return False, None


def get_pro_user_selections():
    """Get user selections for Expert mode.
    Returns dict with user_id and language_code, or None.
    """
    is_authenticated, user_id = render_pro_authentication()

    if not is_authenticated:
        return None

    st.sidebar.markdown("---")
    st.sidebar.info(f"👤 **User:** {user_id}")

    selected_language = st.sidebar.selectbox(
        "Species Name Language",
        options=["Scientific Names"] + list(LANGUAGE_MAPPING.keys()),
        help="Choose the language for species names",
    )

    language_code = (
        "Scientific_Name"
        if selected_language == "Scientific Names"
        else LANGUAGE_MAPPING[selected_language]
    )

    return {
        "user_id": user_id,
        "confidence_threshold": 0.0,
        "language_code": language_code,
    }
