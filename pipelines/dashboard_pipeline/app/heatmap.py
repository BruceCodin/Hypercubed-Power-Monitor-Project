"""UK Power Outage Heatmap Page"""
from datetime import datetime, timedelta
import streamlit as st
from heatmap_helper import (
    get_live_outage_data,
    get_all_outage_data,
    get_filtered_outage_data,
    get_mapped_df,
    create_bubble_map,
    count_outage_status
)
from title_config import title_config


def render_heatmap_page():
    """Render the heatmap page with filters and visualizations."""
    title_config("Power Outage Heatmap")
    st.markdown(
        "Real-time and historical tracking of outages across the UK"
    )

    # --- MODE SELECTION ---

    st.subheader("Data Range")
    mode = st.radio(
        "Time Range",
        ["live", "historical"],
        horizontal=True,
        format_func=lambda x: (
            "Recent (Last 3 Hours)" if x == "live"
            else "Historical (Date Range)"
        )
    )

    # --- LOAD DATA BASED ON MODE ---
    if mode == "live":
        with st.spinner('Fetching live stats...'):
            df = get_live_outage_data()
    else:
        # Historical mode - load all data and let user filter by date
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                datetime.now() - timedelta(days=30)
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                datetime.now() - timedelta(days=1)
            )

        with st.spinner('Fetching historical data...'):
            df_all = get_all_outage_data()
            if df_all.empty:
                st.warning("No historical data found.")
                return
            df = get_filtered_outage_data(df_all, start_date, end_date)

    if df.empty:
        st.warning("No data found.")
        return

    # --- DATA PREPARATION ---
    # Prepare data for filters
    df_mapped = get_mapped_df(df)

    MIN_OUTAGES = 0
    MAX_OUTAGES = int(df_mapped['outage_count'].max())

    # Get available providers
    available_providers = sorted(df['source_provider'].unique().tolist())

    # Set bubble size
    bubble_size = 20  # Fixed bubble size for heatmap
    outage_range = (MIN_OUTAGES, MAX_OUTAGES)

    # Initialize provider filter with session state to capture filter for KPIs
    if "heatmap_providers" not in st.session_state:
        st.session_state.heatmap_providers = available_providers

    st.divider()

    # --- KPI's ---

    st.header("Key Metrics")

    # Display KPIs with currently selected providers
    df_filtered = df_mapped[
        (df_mapped['outage_count'] >= outage_range[0]) &
        (df_mapped['outage_count'] <= outage_range[1]) &
        (df['source_provider'].isin(st.session_state.heatmap_providers))
    ]

    status_counts = count_outage_status(df_filtered)
    total_outages = len(df_filtered.drop_duplicates(subset=['postcode']))

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("📍 Total Postcodes Affected", total_outages)
    kpi2.metric("📅 Planned Outages", status_counts['planned'])
    kpi3.metric("⚠️ Unplanned Outages", status_counts['unplanned'])

    st.divider()

    # --- PAGE FILTERS (just above heatmap) ---
    st.subheader("Provider Filter")

    # Filter by provider
    selected_providers = st.multiselect(
        "Provider",
        options=available_providers,
        default=available_providers,
        help="Select power companies to display",
        key="heatmap_providers"
    )

    # --- HEATMAP ---
    # Filter dataframe based on user selection
    df_filtered = df_mapped[
        (df_mapped['outage_count'] >= outage_range[0]) &
        (df_mapped['outage_count'] <= outage_range[1]) &
        (df['source_provider'].isin(selected_providers))
    ]

    # Create and display bubble map
    fig = create_bubble_map(df_filtered, bubble_size)
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    # For standalone testing
    st.set_page_config(page_title="UK Power Outage Heatmap", layout="wide")
    st.title("⚡ UK Power Outage Heatmap")
    render_heatmap_page()
