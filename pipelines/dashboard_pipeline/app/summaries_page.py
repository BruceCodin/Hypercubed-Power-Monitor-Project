"""
Streamlit dashboard page for AI-generated energy summaries.
Displays latest and historical AI summaries in a blog-style format.
"""
# pylint: disable = W1309

from datetime import datetime
import logging
import streamlit as st
from load_summaries import (
    get_latest_summary,
    list_all_summaries,
    get_summary_by_key,
    format_timestamp
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def render_summaries_page():
    """Render the AI Energy Summaries dashboard page."""
    # Page header
    st.title("AI Energy Summaries")
    st.markdown("AI-powered analysis of UK energy data, updated every 6 hours")
    st.markdown("AI can always make mistakes. Please double check responses")

    # Main content area
    tab1, tab2 = st.tabs(["Latest Summary", "Summary History"])

    # Tab 1: Latest Summary
    with tab1:
        st.subheader("Most Recent Analysis")

        latest_summary = get_latest_summary()

        if latest_summary:
            # Display timestamp
            timestamp = datetime.fromisoformat(latest_summary['timestamp'])
            st.caption(f"Generated: {format_timestamp(timestamp)}")

            # Display summary in a nice card
            with st.container():
                st.markdown(f"### Summary")
                st.write(latest_summary['summary'])

            st.divider()

            # Display key metrics in columns
            st.markdown("### Key Metrics")

            data = latest_summary.get('data', {})

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="⚡ Total Generation",
                    value=f"{data.get('generation', {}).get('total_generation_mw', 0):,.0f} MW"
                )

            with col2:
                st.metric(
                    label="🌍 Carbon Intensity",
                    value=f"{data.get('carbon', {}).get('average_intensity', 0)} gCO2/kWh",
                    delta=None,
                    delta_color="inverse"
                )

            with col3:
                st.metric(
                    label="💷 Average Price",
                    value=f"£{data.get('pricing', {}).get('average_price', 0)}/MWh"
                )

            with col4:
                outages = data.get('outages', {})
                st.metric(
                    label="⚠️ Power Outages",
                    value=f"{outages.get('total_outages', 0)}",
                    delta=f"{outages.get('unplanned', 0)} unplanned",
                    delta_color="inverse"
                )

            # Expander for detailed data
            with st.expander("View Detailed Data"):
                st.json(data)

        else:
            st.warning(
                "⚠️ Could not load the latest summary. Please try refreshing.")

    # Tab 2: Summary History
    with tab2:
        st.subheader("Previous Summaries")

        # Number of summaries to show
        num_summaries = st.slider(
            "Number of summaries to display:",
            min_value=5,
            max_value=50,
            value=10,
            step=5
        )

        summaries = list_all_summaries(max_summaries=num_summaries)

        if summaries:
            st.info(f"Showing {len(summaries)} most recent summaries")

            # Display summaries in blog-style format
            for summary_meta in summaries:
                with st.container():
                    # Fetch the full summary
                    summary_data = get_summary_by_key(summary_meta['s3_key'])

                    if summary_data:
                        # Header with timestamp
                        col1, col2 = st.columns([5, 1])
                        with col1:
                            st.markdown(
                                f"#### {format_timestamp(summary_meta['timestamp'])}")
                        with col2:
                            st.caption(
                                f"{summary_meta['timestamp'].strftime('%H:%M')}")

                        # Summary text
                        st.info(summary_data['summary'])

                        # Quick stats
                        data = summary_data.get('data', {})
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.caption(
                                f"⚡ {data.get('generation', {}).get('total_generation_mw', 0):,.0f} MW")
                        with col2:
                            st.caption(
                                f"{data.get('carbon', {}).get('average_intensity', 0)} gCO2/kWh")
                        with col3:
                            st.caption(
                                f"£{data.get('pricing', {}).get('average_price', 0)}/MWh")

                        st.divider()
                    else:
                        st.error(
                            f"Failed to load summary from {summary_meta['timestamp']}")
        else:
            st.warning("⚠️ No historical summaries found.")

    # Footer
    st.divider()
    st.caption(
        "Summaries are automatically generated every 6 hours using AI analysis of UK energy data")


if __name__ == "__main__":
    st.set_page_config(
        page_title="AI Energy Summaries",
        page_icon="🤖",
        layout="wide"
    )
    render_summaries_page()
