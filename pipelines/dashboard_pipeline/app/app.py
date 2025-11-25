"""Main dashboard page for Power Monitor application."""

import streamlit as st
import sys
from pathlib import Path

st.set_page_config(
    page_title="Power Monitor Dashboard",
    page_icon="⚡",
    layout="wide"
)

st.title("UK Power Monitor Dashboard")

# Create tabs for different pages
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Home",
    "🗺️ Outage Heatmap",
    "⚡ Power Generation",
    "🤖 AI Summaries"
])

# --- HOME TAB ---
with tab1:
    st.header("Welcome to UK Power Monitor")
    st.markdown("""
### 📑 Available Pages

- **Outage Heatmap**: View live power outage data across the UK by power provider and outage count


- **Power Generation**: Monitor real-time UK energy generation by fuel type


- **AI Summaries**: AI-powered analysis of UK energy data
""")

st.info("Use the tabs at the top to navigate between different monitoring views. Each page provides interactive controls to customize your view and drill down into specific areas of interest.")

# --- OUTAGE HEATMAP TAB ---
with tab2:
    # Add pages directory to path
    pages_dir = Path(__file__).parent / "pages" / "heatmap"
    sys.path.insert(0, str(pages_dir))

    try:
        from heatmap_helper import (
            get_live_outage_data,
            get_mapped_df,
            create_bubble_map,
            count_outage_status
        )

        st.subheader("UK Power Outage Heatmap")
        st.text("Showing last 3 hours worth of live outage data")

        # Run heatmap logic
        with st.spinner('Fetching live stats...'):
            df = get_live_outage_data()

        if df.empty:
            st.warning("No data found.")
        else:
            # Prepare data for filters
            df_mapped = get_mapped_df(df)

            MIN_OUTAGES = 0
            MAX_OUTAGES = int(df_mapped['outage_count'].max())

            st.subheader("Filters")

            # Create filter columns
            filter_col1, filter_col2, filter_col3 = st.columns(3)

            # Get available providers
            available_providers = sorted(
                df['source_provider'].unique().tolist())

            # Filter by provider
            with filter_col1:
                selected_providers = st.multiselect(
                    "Provider",
                    options=available_providers,
                    default=available_providers,
                    help="Select power companies to display"
                )

            # Let users filter by outage count
            with filter_col2:
                outage_range = st.slider(
                    "Count Filter",
                    min_value=MIN_OUTAGES,
                    max_value=MAX_OUTAGES,
                    value=(MIN_OUTAGES, MAX_OUTAGES),
                    step=1
                )

            # Let users adjust bubble size
            with filter_col3:
                bubble_size = st.slider(
                    "Bubble Radius",
                    min_value=10,
                    max_value=50,
                    value=30,
                    step=1
                )

            # Filter dataframe based on user selection
            df_filtered = df_mapped[
                (df_mapped['outage_count'] >= outage_range[0]) &
                (df_mapped['outage_count'] <= outage_range[1]) &
                (df['source_provider'].isin(selected_providers))
            ]

            # --- KPI's ---
            status_counts = count_outage_status(df_filtered)
            total_outages = len(
                df_filtered.drop_duplicates(subset=['postcode']))

            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("📍 Total Postcodes Affected", total_outages)
            kpi2.metric("📅 Planned Outages", status_counts['planned'])
            kpi3.metric("⚠️ Unplanned Outages", status_counts['unplanned'])

            st.divider()

            # --- HEATMAP ---
            fig = create_bubble_map(df_filtered, bubble_size)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading heatmap: {str(e)}")

# --- POWER GENERATION TAB ---
with tab3:
    power_gen_dir = Path(__file__).parent / "pages" / "power_generation"
    sys.path.insert(0, str(power_gen_dir))

    try:
        from dashboard_power_generation_helpers import (
            get_secrets,
            get_power_data,
            create_generation_chart,
            create_fuel_breakdown_chart,
            create_carbon_intensity_chart,
            create_price_chart,
            create_demand_chart,
            create_carbon_demand_chart
        )
        from datetime import datetime, timedelta

        st.subheader("UK Power Supply Monitor")
        st.markdown(
            "Real-time and historical tracking of power supply, "
            "carbon intensity, and system pricing"
        )

        try:
            secrets = get_secrets()

            # Filters
            st.subheader("Filters")
            col1, col2 = st.columns(2)

            with col1:
                time_filter = st.radio(
                    "Time Range",
                    ["recent", "historical"],
                    format_func=lambda x: (
                        "Recent (24 hours)" if x == "recent"
                        else "Historical (30 days)"
                    )
                )

            with col2:
                data_type = st.radio(
                    "Data Type",
                    ["generation", "carbon", "price", "demand"],
                    format_func=lambda x: {
                        "generation": "Power Generation",
                        "carbon": "Carbon Intensity",
                        "price": "System Pricing",
                        "demand": "Power Demand"
                    }[x]
                )

            # Fetch data
            start_date = (
                datetime.now() - timedelta(days=1)
                if time_filter == "recent"
                else datetime.now() - timedelta(days=30)
            )

            df = get_power_data(secrets, start_date)

            if df.empty:
                st.warning("No data available for selected period")
            else:
                # Display selected chart
                if data_type == "generation":
                    st.plotly_chart(
                        create_generation_chart(df),
                        use_container_width=True
                    )
                    st.plotly_chart(
                        create_fuel_breakdown_chart(df),
                        use_container_width=True
                    )
                elif data_type == "carbon":
                    st.plotly_chart(
                        create_carbon_intensity_chart(df),
                        use_container_width=True
                    )
                    st.plotly_chart(
                        create_carbon_demand_chart(df),
                        use_container_width=True
                    )
                elif data_type == "price":
                    st.plotly_chart(
                        create_price_chart(df),
                        use_container_width=True
                    )
                else:
                    st.plotly_chart(
                        create_demand_chart(df),
                        use_container_width=True
                    )

        except (ConnectionError, ValueError) as error:
            st.error(f"Error loading data: {error}")

    except Exception as e:
        st.error(f"Error loading power generation page: {str(e)}")

# --- AI SUMMARIES TAB ---
with tab4:
    summaries_dir = Path(__file__).parent / "pages" / "ai_summaries_page"
    sys.path.insert(0, str(summaries_dir))

    try:
        from load_summaries import (
            get_latest_summary,
            list_all_summaries,
            get_summary_by_key,
            format_timestamp
        )
        import logging

        # Configure logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        st.subheader("AI Energy Summaries")
        st.markdown(
            "AI-powered analysis of UK energy data, updated every 6 hours")

        # Add refresh button
        col1, col2 = st.columns([6, 1])
        with col2:
            if st.button("🔄 Refresh", use_container_width=True, key="refresh_summaries"):
                st.cache_data.clear()
                st.rerun()

        st.divider()

        # Main content area
        summary_tab1, summary_tab2 = st.tabs([
            "Latest Summary",
            "Summary History"
        ])

        # Tab 1: Latest Summary
        with summary_tab1:
            st.subheader("Most Recent Analysis")

            latest_summary = get_latest_summary()

            if latest_summary:
                st.markdown(
                    f"**Generated:** {format_timestamp(latest_summary.get('timestamp', 'Unknown'))}")
                st.markdown(latest_summary.get(
                    'summary', 'No summary available'))
            else:
                st.info("No summaries available yet")

        # Tab 2: Summary History
        with summary_tab2:
            st.subheader("Historical Summaries")

            all_summaries = list_all_summaries()

            if all_summaries:
                summary_keys = list(all_summaries.keys())
                selected_key = st.selectbox(
                    "Select a summary",
                    summary_keys,
                    format_func=lambda x: format_timestamp(x)
                )

                if selected_key:
                    summary = get_summary_by_key(selected_key)
                    if summary:
                        st.markdown(
                            f"**Generated:** {format_timestamp(summary.get('timestamp', 'Unknown'))}")
                        st.markdown(summary.get(
                            'summary', 'No summary available'))
            else:
                st.info("No historical summaries available")

    except Exception as e:
        st.error(f"Error loading AI summaries: {str(e)}")
