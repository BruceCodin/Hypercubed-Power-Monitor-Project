import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime, timedelta

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Grid Operations Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- DATABASE CONFIGURATION ---
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "muarijmuarij",
    "password": "abc123",
    "port": "5432"
}

# --- DATA CONNECTION LAYER ---


# Cache data for 5 minutes to prevent overwhelming the DB on every interaction
@st.cache_data(ttl=300)
def load_data():
    """
    Connects to the PostgreSQL database and fetches the complete dataset
    joining Settlement, Generation, Price, and Carbon metrics.
    """
    query = """
    SELECT 
        s.settlement_id,
        s.settlement_date,
        s.settlement_period,
        ft.fuel_type,
        g.generation as generation_mw,
        sp.system_sell_price,
        ci.intensity_actual,
        -- Coalesce recent and historical demand tables to get a single demand figure
        COALESCE(rd.national_demand, hd.national_demand) as national_demand
    FROM "settlement" s
    JOIN "generation" g ON s.settlement_id = g.settlement_id
    JOIN "fuel_type" ft ON g.fuel_id = ft.id
    LEFT JOIN "system_price" sp ON s.settlement_id = sp.settlement_id
    LEFT JOIN "carbon_intensity" ci ON s.settlement_id = ci.settlement_id
    LEFT JOIN "recent_demand" rd ON s.settlement_id = rd.settlement_id
    LEFT JOIN "historical_demand" hd ON s.settlement_id = hd.settlement_id
    ORDER BY s.settlement_date DESC;
    """

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()

        # Ensure correct data types
        df['settlement_date'] = pd.to_datetime(df['settlement_date'])
        df['system_sell_price'] = pd.to_numeric(df['system_sell_price'])

        return df

    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.info(
            "Check your 'DB_CONFIG' settings in the code. Ensure your password matches DBeaver.")
        # Return an empty dataframe structure to prevent app crash if DB is down
        return pd.DataFrame(columns=[
            'settlement_id', 'settlement_date', 'settlement_period',
            'fuel_type', 'generation_mw', 'system_sell_price',
            'intensity_actual', 'national_demand'
        ])


# --- LOAD DATA ---
# We use a spinner to indicate the DB query is running
with st.spinner('Fetching live data from Grid Database...'):
    df = load_data()

# Stop execution if data failed to load
if df.empty:
    st.warning(
        "No data available. Please check your database connection configuration.")
    st.stop()

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("⚡ GridOps Analytics")
page = st.sidebar.radio(
    "Navigate", ["Live Overview (Today)", "Historical Analysis"])

st.sidebar.markdown("---")
# Show the latest data timestamp from the DB
last_update = df['settlement_date'].max() if not df.empty else datetime.now()
st.sidebar.caption(
    f"Data Last Updated: {last_update.strftime('%Y-%m-%d %H:%M')}")

# --- PAGE 1: LIVE OVERVIEW ---
if page == "Live Overview (Today)":
    st.title("📊 Live Grid Overview")
    st.markdown("Real-time monitoring of grid settlement periods for today.")

    # Filter for Today
    today_date = df['settlement_date'].max().date()
    today_df = df[df['settlement_date'].dt.date == today_date]

    if today_df.empty:
        st.info("No data found for today yet.")
    else:
        # Get most recent settlement period
        latest_period = today_df['settlement_date'].max()
        latest_data = today_df[today_df['settlement_date'] == latest_period]

        # KPI Metrics
        col1, col2, col3, col4 = st.columns(4)

        current_demand = latest_data['national_demand'].iloc[0] if not latest_data['national_demand'].isnull(
        ).all() else 0
        avg_price = latest_data['system_sell_price'].iloc[0] if not latest_data['system_sell_price'].isnull(
        ).all() else 0
        carbon_intensity = latest_data['intensity_actual'].iloc[0] if not latest_data['intensity_actual'].isnull(
        ).all() else 0
        total_gen = latest_data['generation_mw'].sum()

        col1.metric("Grid Demand", f"{current_demand:,.0f} MW")
        col2.metric("System Price", f"£{avg_price:.2f}")
        col3.metric("Carbon Intensity", f"{carbon_intensity:.0f} gCO2/kWh")
        col4.metric("Total Generation", f"{total_gen:,.0f} MW")

        st.markdown("### ⚡ Current Generation Mix")

        # Generation Mix Donut Chart
        fig_mix = px.pie(
            latest_data,
            values='generation_mw',
            names='fuel_type',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Prism,
            title=f"Fuel Mix for Period {latest_period.strftime('%H:%M')}"
        )
        fig_mix.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_mix, use_container_width=True)

        st.markdown("### 📈 Intraday Trends")

        # Prepare Intraday Data (Pivot for plotting)
        intraday_pivot = today_df.pivot_table(
            index='settlement_date',
            columns='fuel_type',
            values='generation_mw',
            aggfunc='sum'
        ).reset_index()

        # Add Demand and Price to the pivot for context
        meta_data = today_df[[
            'settlement_date', 'national_demand', 'system_sell_price']].drop_duplicates()
        intraday_viz = pd.merge(
            intraday_pivot, meta_data, on='settlement_date')

        tab1, tab2 = st.tabs(["System Price vs Demand", "Generation Stack"])

        with tab1:
            # Dual Axis Chart
            fig_dual = go.Figure()
            fig_dual.add_trace(go.Scatter(
                x=intraday_viz['settlement_date'],
                y=intraday_viz['national_demand'],
                name='National Demand (MW)',
                line=dict(color='#FF4B4B', width=3)
            ))
            fig_dual.add_trace(go.Scatter(
                x=intraday_viz['settlement_date'],
                y=intraday_viz['system_sell_price'],
                name='System Price (£)',
                yaxis='y2',
                line=dict(color='#1E88E5', dash='dot')
            ))
            fig_dual.update_layout(
                yaxis=dict(title='Demand (MW)'),
                yaxis2=dict(title='Price (£)', overlaying='y', side='right'),
                title="Intraday Demand vs Price",
                hovermode="x unified"
            )
            st.plotly_chart(fig_dual, use_container_width=True)

        with tab2:
            # Stacked Area Chart
            fuel_cols = [c for c in intraday_viz.columns if c not in [
                'settlement_date', 'national_demand', 'system_sell_price']]
            fig_stack = px.area(
                intraday_viz,
                x='settlement_date',
                y=fuel_cols,
                title="Generation Stack (MW)",
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            st.plotly_chart(fig_stack, use_container_width=True)

# --- PAGE 2: HISTORICAL ANALYSIS ---
elif page == "Historical Analysis":
    st.title("📅 Historical Performance Analysis")
    st.markdown(
        "Analyze long-term trends in generation, pricing, and carbon intensity.")

    # Sidebar Filters
    st.sidebar.header("Filters")

    # Exclude today from history
    today_date = df['settlement_date'].max().date()
    history_df = df[df['settlement_date'].dt.date < today_date]

    if history_df.empty:
        st.warning("No historical data available (only today's data found).")
    else:
        min_date = history_df['settlement_date'].min().date()
        max_date = history_df['settlement_date'].max().date()

        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        if len(date_range) == 2:
            start, end = date_range
            mask = (history_df['settlement_date'].dt.date >= start) & (
                history_df['settlement_date'].dt.date <= end)
            filtered_df = history_df[mask]
        else:
            filtered_df = history_df

        # Aggregate Data by Day
        # Note: we must aggregate carefully because 'generation_mw' is per fuel type,
        # while price/demand are per settlement period.

        # 1. First aggregate generation by settlement to get total generation per period
        # Then average that for the day? Or sum generation for the whole day.
        # Let's Sum generation for the day, and Average Price/Carbon for the day.

        daily_metrics = filtered_df[['settlement_date', 'system_sell_price',
                                     'intensity_actual', 'national_demand']].drop_duplicates()
        # ADDED numeric_only=True to prevent date column from lingering and clashing with index
        daily_agg_metrics = daily_metrics.groupby(
            daily_metrics['settlement_date'].dt.date).mean(numeric_only=True)

        daily_gen = filtered_df.groupby(filtered_df['settlement_date'].dt.date)[
            'generation_mw'].sum()

        daily_summary = pd.concat(
            [daily_agg_metrics, daily_gen], axis=1).reset_index()
        daily_summary.columns = [
            'settlement_date', 'avg_price', 'intensity_actual', 'avg_demand', 'total_gen']

        # Row 1: Price & Carbon Heatmap
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 💰 Price Volatility")
            fig_price = px.line(
                daily_summary,
                x='settlement_date',
                y='avg_price',
                title="Average Daily System Price (£)",
                line_shape='spline'
            )
            fig_price.update_traces(line_color='#2E7D32', fill='tozeroy')
            st.plotly_chart(fig_price, use_container_width=True)

        with col2:
            st.markdown("### 🌿 Carbon Intensity Trend")
            fig_carbon = px.bar(
                daily_summary,
                x='settlement_date',
                y='intensity_actual',
                title="Daily Average Carbon Intensity",
                color='intensity_actual',
                # Red is high carbon (bad), Green is low
                color_continuous_scale='RdYlGn_r'
            )
            st.plotly_chart(fig_carbon, use_container_width=True)

        # Row 2: Generation Mix Evolution
        st.markdown("### 🏭 Generation Source Evolution")

        # Resample to daily for cleaner stacked bar
        daily_fuel_mix = filtered_df.groupby([filtered_df['settlement_date'].dt.date, 'fuel_type'])[
            'generation_mw'].sum().reset_index()
        daily_fuel_mix.columns = ['date', 'fuel_type', 'generation_mw']

        fig_evol = px.bar(
            daily_fuel_mix,
            x='date',
            y='generation_mw',
            color='fuel_type',
            title="Daily Generation Mix by Fuel Type",
            color_discrete_sequence=px.colors.qualitative.Prism
        )
        st.plotly_chart(fig_evol, use_container_width=True)

        # Row 3: Correlations
        st.markdown("### 🔗 Market Correlations")
        corr_col1, corr_col2 = st.columns(2)

        with corr_col1:
            # Is Price driven by Demand?
            fig_scat1 = px.scatter(
                filtered_df,
                x='national_demand',
                y='system_sell_price',
                color='intensity_actual',
                title="System Price vs National Demand",
                labels={
                    'national_demand': 'Demand (MW)', 'system_sell_price': 'Price (£)'},
                opacity=0.5
            )
            st.plotly_chart(fig_scat1, use_container_width=True)

        with corr_col2:
            # Does Wind lower the price?
            wind_data = filtered_df[filtered_df['fuel_type'] == 'Wind']
            fig_scat2 = px.scatter(
                wind_data,
                x='generation_mw',
                y='system_sell_price',
                title="System Price vs Wind Generation",
                labels={
                    'generation_mw': 'Wind Gen (MW)', 'system_sell_price': 'Price (£)'},
                opacity=0.5,
                trendline="ols"
            )
            st.plotly_chart(fig_scat2, use_container_width=True)
