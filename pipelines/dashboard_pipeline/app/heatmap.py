"""UK Power Outage Heatmap Page"""
import streamlit as st
from heatmap_helper import (
    get_live_outage_data,
    get_mapped_df,
    create_bubble_map,
    count_outage_status
)


def render_heatmap_page():
    """Render the heatmap page with filters and visualizations."""
    st.title("Power Outage Heatmap")
    st.text("Showing last 3 hours worth of live outage data")

    # --- VISUALIZATION ---
    with st.spinner('Fetching live stats...'):
        df = get_live_outage_data()

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
    st.subheader("Filters")

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
