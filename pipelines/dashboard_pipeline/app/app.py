"""Main dashboard page for Power Monitor application."""

import streamlit as st
from streamlit_option_menu import option_menu

# Set page config first
st.set_page_config(
    page_title="Power Monitor Dashboard",
    page_icon="⚡",
    layout="wide"
)

# Import render functions
try:
    from heatmap import render_heatmap_page
    from dashboard_power_generation import render_power_generation_page
    from summaries_page import render_summaries_page
    from page_form import render_subscription_form_page
except ImportError as e:
    st.error(f"Error importing page modules: {e}")
    st.stop()

# Initialize session state for active page
if "active_page" not in st.session_state:
    st.session_state.active_page = "home"

# Sidebar navigation using option_menu
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",
        options=["Home", "Generation", "Outage Map", "Summaries", "Subscribe"],
        icons=["house", "lightning-charge", "map",
               "chat-square-text", "pencil-square"],
        menu_icon="cast",
        default_index=0,
    )

# Map selected menu item to page
page_mapping = {
    "Home": "home",
    "Outage Map": "heatmap",
    "Generation": "power_generation",
    "Summaries": "summaries",
    "Subscribe": "subscription"
}

st.session_state.active_page = page_mapping.get(selected, "home")

# Render the appropriate page based on active_page
if st.session_state.active_page == "home":
    st.title("Welcome to UK Power Monitor")
    st.markdown("""
This comprehensive dashboard provides real-time insights into UK power generation,
carbon intensity, and power outage alerts by consolidating data from multiple providers and sources.
""")

    st.divider()

    st.markdown("""
    ### 📑 Available Pages

    - **Outage Heatmap**: View live power outage data across the UK by power provider and outage count

    - **Power Generation**: Monitor real-time UK energy generation by fuel type and carbon intensity

    - **AI Summaries**: AI-powered analysis of UK energy data

    - **Subscribe**: Sign up for power cut alerts and daily energy summary emails
    """)

    st.divider()

    st.info("Use the navigation buttons in the sidebar to explore different monitoring views. Each page provides interactive controls to customize your view and drill down into specific areas of interest.")

elif st.session_state.active_page == "heatmap":
    try:
        render_heatmap_page()
    except Exception as e:
        st.error(f"Error loading heatmap: {str(e)}")

elif st.session_state.active_page == "power_generation":
    try:
        render_power_generation_page()
    except Exception as e:
        st.error(f"Error loading power generation page: {str(e)}")

elif st.session_state.active_page == "summaries":
    try:
        render_summaries_page()
    except Exception as e:
        st.error(f"Error loading AI summaries: {str(e)}")

elif st.session_state.active_page == "subscription":
    try:
        render_subscription_form_page()
    except Exception as e:
        st.error(f"Error loading subscription form: {str(e)}")
