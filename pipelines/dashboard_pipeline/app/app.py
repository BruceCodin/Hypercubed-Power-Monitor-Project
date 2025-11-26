"""Main dashboard page for Power Monitor application."""

import streamlit as st

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

# Main title
st.title("⚡ UK Power Monitor Dashboard")

# Sidebar navigation
st.sidebar.header("Navigation")

# Create compact button-based navigation in sidebar using columns
nav_col1, nav_col2 = st.sidebar.columns(2)
with nav_col1:
    if st.button("Home", use_container_width=True, key="nav_home"):
        st.session_state.active_page = "home"
with nav_col2:
    if st.button("Heatmap", use_container_width=True, key="nav_heatmap"):
        st.session_state.active_page = "heatmap"

nav_col3, nav_col4 = st.sidebar.columns(2)
with nav_col3:
    if st.button("Generation", use_container_width=True, key="nav_generation"):
        st.session_state.active_page = "power_generation"
with nav_col4:
    if st.button("Summaries", use_container_width=True, key="nav_summaries"):
        st.session_state.active_page = "summaries"

nav_col5, nav_col6 = st.sidebar.columns(2)
with nav_col5:
    if st.button("Subscribe", use_container_width=True, key="nav_subscribe"):
        st.session_state.active_page = "subscription"
with nav_col6:
    pass

# Render the appropriate page based on active_page
if st.session_state.active_page == "home":
    st.header("Welcome to UK Power Monitor")
    st.markdown("""
This comprehensive dashboard provides real-time insights into UK power generation,
carbon intensity, and power outage alerts. Monitor and analyze energy data across
multiple dimensions to stay informed about the UK power system.

### 📑 Available Pages

- **Outage Heatmap**: View live power outage data across the UK by power provider and outage count

- **Power Generation**: Monitor real-time UK energy generation by fuel type and carbon intensity

- **AI Summaries**: AI-powered analysis of UK energy data
""")
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
