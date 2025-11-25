"""
UK Power Supply Monitor Dashboard

A Streamlit application for monitoring real-time and historical UK power
generation, carbon intensity, and system pricing data.
"""

import json
import os
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import boto3
from dashboard_power_generation_helpers import (
    get_power_data,
    create_generation_chart,
    create_fuel_breakdown_chart,
    create_carbon_intensity_chart,
    create_price_chart,
    create_demand_chart,
    create_carbon_demand_chart
)

# AWS Secrets Manager configuration
SECRETS_ARN = (
    "arn:aws:secretsmanager:eu-west-2:129033205317:"
    "secret:c20-power-monitor-db-credentials-TAc5Xx"
)


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager."""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=SECRETS_ARN)
    secret = response['SecretString']
    return json.loads(secret)


def load_secrets_to_env(credentials: dict):
    """Load database credentials into environment variables."""
    for key, value in credentials.items():
        os.environ[key] = str(value)


def connect_to_database() -> psycopg2.extensions.connection:
    """Connect to AWS Postgres database."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT")),
    )

# Streamlit App
st.set_page_config(
    page_title="UK Power Supply Monitor",
    page_icon="⚡",
    layout="wide"
)

# Initialize connection
try:
    secrets = get_secrets()
    load_secrets_to_env(secrets)
except (ConnectionError, ValueError) as error:
    st.error(f"Error loading credentials: {error}")
    st.stop()

# Header
st.title("UK Power Supply Monitor")
st.markdown(
    "Real-time and historical tracking of power supply, "
    "carbon intensity, and system pricing"
)

# Sidebar filters
st.sidebar.header("Filters")

time_filter = st.sidebar.radio(
    "Time Range",
    ["recent", "historical"],
    format_func=lambda x: (
        "Recent (Last 24 hours)" if x == "recent"
        else "Historical (> 24 hours)"
    )
)

# Date range for historical data
if time_filter == "historical":
    col1, col2 = st.sidebar.columns(2)
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
else:
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()

# Fetch data and apply filters
try:
    with st.spinner("Loading data..."):
        df_initial = get_power_data(time_filter, start_date, end_date)

    if df_initial.empty:
        st.warning("No data available for the selected time range.")
        st.stop()

    # Fuel type filter
    st.sidebar.markdown("---")
    st.sidebar.subheader("Fuel Type Filter")

    available_fuel_types = sorted(df_initial['fuel_type'].unique().tolist())
    selected_fuel_types = st.sidebar.multiselect(
        "Select fuel types to display",
        options=available_fuel_types,
        default=available_fuel_types,
        help=(
            "Choose which fuel types to include in the analysis. "
            "Charts will show only selected types."
        )
    )

    if not selected_fuel_types:
        st.sidebar.warning("Select at least one fuel type")
        st.error("Please select at least one fuel type.")
        st.stop()

    st.sidebar.caption(
        "Tip: Select 2-3 fuel types to compare their relative relationship"
    )

    # Filter data by selected fuel types
    df = df_initial[df_initial['fuel_type'].isin(selected_fuel_types)].copy()

    # Show selection count
    st.sidebar.success(
        f"{len(selected_fuel_types)} of {len(available_fuel_types)} "
        "fuel types selected"
    )

    # Key Metrics
    st.header("Key Metrics")

    has_demand = 'national_demand' in df.columns

    if time_filter == "recent":
        # Recent mode - show total power and averages
        col1, col2, col3 = st.columns(3)

        # Calculate totals and averages across all data
        period_data = df.groupby(['settlement_date', 'settlement_period']).agg({
            'generation_mw': 'sum',
            'intensity_actual': 'first',
            'system_sell_price': 'first'
        }).reset_index()

        with col1:
            total_power = period_data['generation_mw'].sum()
            st.metric("Total Power Supply", f"{total_power:,.0f} MW")

        with col2:
            avg_intensity = period_data['intensity_actual'].mean()
            st.metric("Avg Carbon Intensity", f"{avg_intensity:.0f} gCO2/kWh")

        with col3:
            avg_price = period_data['system_sell_price'].mean()
            st.metric("Avg System Price", f"£{avg_price:.2f}/MWh")
    else:
        # Historical mode - always show totals and averages
        if has_demand:
            col1, col2, col3, col4 = st.columns(4)
        else:
            col1, col2, col3 = st.columns(3)

        # Group by date and period, sum fuel types, then calculate totals
        agg_dict = {
            'generation_mw': 'sum',
            'intensity_actual': 'first',
            'system_sell_price': 'first'
        }
        if has_demand:
            agg_dict['national_demand'] = 'first'

        period_data = df.groupby(
            ['settlement_date', 'settlement_period']
        ).agg(agg_dict).reset_index()

        with col1:
            total_supply = period_data['generation_mw'].sum()
            st.metric("Total Power Supply", f"{total_supply:,.0f} MW")

        with col2:
            avg_intensity = period_data['intensity_actual'].mean()
            st.metric("Avg Carbon Intensity", f"{avg_intensity:.0f} gCO2/kWh")

        with col3:
            avg_price = period_data['system_sell_price'].mean()
            st.metric("Avg System Price", f"£{avg_price:.2f}/MWh")

        if has_demand:
            with col4:
                avg_demand = period_data['national_demand'].mean()
                st.metric("Avg National Demand", f"{avg_demand:,.0f} MW")

    # Charts
    st.header("Visualizations")

    # Power Supply
    st.subheader("Power Supply by Source")
    gen_chart = create_generation_chart(df)
    st.altair_chart(gen_chart, use_container_width=True)

    # Fuel Mix and Stats
    col1, col2 = st.columns([1, 2])

    with col1:
        fuel_chart = create_fuel_breakdown_chart(df)
        st.altair_chart(fuel_chart, use_container_width=True)

    with col2:
        st.subheader("Supply Statistics")
        fuel_stats = df.groupby(
            'fuel_type'
        )['generation_mw'].agg(['mean', 'max', 'min']).round(2)
        fuel_stats.columns = ['Average (MW)', 'Max (MW)', 'Min (MW)']
        fuel_stats = fuel_stats.sort_values('Average (MW)', ascending=False)
        st.dataframe(fuel_stats, use_container_width=True)

    # Carbon Intensity
    st.subheader("Carbon Intensity")
    carbon_chart = create_carbon_intensity_chart(df)
    st.altair_chart(carbon_chart, use_container_width=True)

    # Price
    st.subheader("System Sell Price")
    price_chart = create_price_chart(df)
    st.altair_chart(price_chart, use_container_width=True)

    # Demand - only show for historical data
    if 'national_demand' in df.columns:
        st.subheader("Power Demand")
        demand_chart = create_demand_chart(df)
        st.altair_chart(demand_chart, use_container_width=True)

        # Add Demand vs Carbon Intensity chart
        st.subheader("Demand vs Carbon Intensity")
        carbon_demand_chart = create_carbon_demand_chart(df)
        if carbon_demand_chart:
            st.altair_chart(carbon_demand_chart, use_container_width=True)

    # Data Table
    with st.expander("View Raw Data"):
        st.dataframe(
            df.sort_values(
                ['settlement_date', 'settlement_period'],
                ascending=False
            ),
            use_container_width=True
        )

    # Auto-refresh for recent data
    if time_filter == "recent":
        st.sidebar.markdown("---")
        st.sidebar.info(
            "Dashboard auto-refreshes every 5 minutes for live supply, "
            "carbon intensity, and pricing data"
        )

except (ConnectionError, ValueError) as e:
    st.error(f"Error loading data: {e}")
    st.exception(e)