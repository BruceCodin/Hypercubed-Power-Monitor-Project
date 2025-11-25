import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import boto3
import json
import os
from datetime import datetime, timedelta

# AWS Secrets Manager configuration
SECRETS_ARN = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager."""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=SECRETS_ARN)
    secret = response['SecretString']
    return json.loads(secret)


def load_secrets_to_env(secrets: dict):
    """Load database credentials into environment variables."""
    for key, value in secrets.items():
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


@st.cache_data(ttl=300)
def get_power_data(time_filter: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch power generation, carbon intensity, and price data."""
    
    # Always use recent_demand table, but only join for historical data
    include_demand = time_filter == "historical"
    
    # Determine date filter
    if time_filter == "recent":
        date_condition = f"s.settlement_date >= NOW() - INTERVAL '24 hours'"
    else:
        date_condition = f"s.settlement_date >= '{start_date}' AND s.settlement_date < '{end_date}'::date + INTERVAL '1 day'"
    
    # Build query with conditional demand join
    demand_join = ""
    demand_columns = ""
    if include_demand:
        demand_join = "INNER JOIN recent_demand d ON s.settlement_id = d.settlement_id"
        demand_columns = ",\n        d.national_demand,\n        d.transmission_system_demand"
    
    query = f"""
    SELECT 
        s.settlement_id,
        s.settlement_date,
        s.settlement_period,
        ci.intensity_forecast,
        ci.intensity_actual,
        ci.intensity_index,
        sp.system_sell_price,
        ft.fuel_type,
        g.generation_mw{demand_columns}
    FROM settlements s
    INNER JOIN carbon_intensity ci ON s.settlement_id = ci.settlement_id
    INNER JOIN system_price sp ON s.settlement_id = sp.settlement_id
    INNER JOIN generation g ON s.settlement_id = g.settlement_id
    INNER JOIN fuel_type ft ON g.fuel_type_id = ft.fuel_type_id
    {demand_join}
    WHERE {date_condition}
        AND ci.intensity_forecast IS NOT NULL
        AND ci.intensity_actual IS NOT NULL
        AND sp.system_sell_price IS NOT NULL
        AND g.generation_mw IS NOT NULL
    ORDER BY s.settlement_date DESC, s.settlement_period DESC, ft.fuel_type;
    """
    
    conn = connect_to_database()
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    df['settlement_date'] = pd.to_datetime(df['settlement_date'])
    
    # Map interconnector codes to friendly country names
    interconnector_mapping = {
        'INTELEC': 'Imports (France)',
        'INTFR': 'Imports (France)',
        'INTIFA2': 'Imports (France)',
        'INTEW': 'Imports (Ireland)',
        'INTIRL': 'Imports (Ireland)',
        'INTGRNL': 'Imports (Ireland)',
        'INTNED': 'Imports (Netherlands)',
        'INTNEM': 'Imports (Belgium)',
        'INTNSL': 'Imports (Norway)',
        'INTVKL': 'Imports (Denmark)'
    }
    
    df['fuel_type'] = df['fuel_type'].replace(interconnector_mapping)
    
    return df


def settlement_to_time(period: int) -> str:
    """Convert settlement period (1-48) to time string."""
    hours = (period - 1) // 2
    minutes = ((period - 1) % 2) * 30
    return f"{hours:02d}:{minutes:02d}"


def create_generation_chart(df: pd.DataFrame) -> alt.Chart:
    """Create stacked area chart for power supply by fuel type."""
    
    # Check if data spans multiple days
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    if unique_dates == 1:
        # Single day - use settlement periods
        gen_data = df.groupby(['settlement_period', 'fuel_type'])['generation_mw'].sum().reset_index()
        gen_data['time_label'] = gen_data['settlement_period'].apply(settlement_to_time)
        
        chart = alt.Chart(gen_data).mark_area().encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('generation_mw:Q', title='Supply (MW)', stack='zero'),
            color=alt.Color('fuel_type:N', title='Fuel Type', scale=alt.Scale(scheme='category20')),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('fuel_type:N', title='Fuel Type'),
                alt.Tooltip('generation_mw:Q', title='Supply (MW)', format=',.0f')
            ]
        ).properties(
            width=700,
            height=400,
            title='Power Supply by Source'
        ).interactive()
    else:
        # Multiple days - use datetime
        gen_data = df.groupby(['settlement_date', 'fuel_type'])['generation_mw'].sum().reset_index()
        
        chart = alt.Chart(gen_data).mark_area().encode(
            x=alt.X('settlement_date:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('generation_mw:Q', title='Supply (MW)', stack='zero'),
            color=alt.Color('fuel_type:N', title='Fuel Type', scale=alt.Scale(scheme='category20')),
            tooltip=[
                alt.Tooltip('settlement_date:T', title='Date', format='%Y-%m-%d'),
                alt.Tooltip('fuel_type:N', title='Fuel Type'),
                alt.Tooltip('generation_mw:Q', title='Supply (MW)', format=',.0f')
            ]
        ).properties(
            width=700,
            height=400,
            title='Power Supply by Source (Multi-Day View)'
        ).interactive()
    
    return chart


def create_carbon_intensity_chart(df: pd.DataFrame) -> alt.Chart:
    """Create line chart for carbon intensity actual vs forecast."""
    
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    if unique_dates == 1:
        # Single day - use settlement periods
        carbon_data = df.groupby('settlement_period').agg({
            'intensity_actual': 'first',
            'intensity_forecast': 'first'
        }).reset_index()
        
        carbon_data['time_label'] = carbon_data['settlement_period'].apply(settlement_to_time)
        
        carbon_long = carbon_data.melt(
            id_vars=['settlement_period', 'time_label'],
            value_vars=['intensity_actual', 'intensity_forecast'],
            var_name='type',
            value_name='intensity'
        )
        
        carbon_long['type'] = carbon_long['type'].map({
            'intensity_actual': 'Actual',
            'intensity_forecast': 'Forecast'
        })
        
        chart = alt.Chart(carbon_long).mark_line(point=True).encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('intensity:Q', title='Carbon Intensity (gCO2/kWh)'),
            color=alt.Color('type:N', title='Type', scale=alt.Scale(domain=['Actual', 'Forecast'], range=['#e74c3c', '#3498db'])),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('intensity:Q', title='Intensity (gCO2/kWh)', format='.2f')
            ]
        ).properties(
            width=700,
            height=300,
            title='Carbon Intensity: Actual vs Forecast'
        ).interactive()
    else:
        # Multiple days - use datetime
        carbon_data = df.groupby('settlement_date').agg({
            'intensity_actual': 'first',
            'intensity_forecast': 'first'
        }).reset_index()
        
        carbon_long = carbon_data.melt(
            id_vars=['settlement_date'],
            value_vars=['intensity_actual', 'intensity_forecast'],
            var_name='type',
            value_name='intensity'
        )
        
        carbon_long['type'] = carbon_long['type'].map({
            'intensity_actual': 'Actual',
            'intensity_forecast': 'Forecast'
        })
        
        chart = alt.Chart(carbon_long).mark_line(point=True).encode(
            x=alt.X('settlement_date:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('intensity:Q', title='Carbon Intensity (gCO2/kWh)'),
            color=alt.Color('type:N', title='Type', scale=alt.Scale(domain=['Actual', 'Forecast'], range=['#e74c3c', '#3498db'])),
            tooltip=[
                alt.Tooltip('settlement_date:T', title='Date', format='%Y-%m-%d'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('intensity:Q', title='Intensity (gCO2/kWh)', format='.2f')
            ]
        ).properties(
            width=700,
            height=300,
            title='Carbon Intensity: Actual vs Forecast'
        ).interactive()
    
    return chart


def create_price_chart(df: pd.DataFrame) -> alt.Chart:
    """Create line chart for system sell price."""
    
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    if unique_dates == 1:
        # Single day - use settlement periods
        price_data = df.groupby('settlement_period')['system_sell_price'].first().reset_index()
        price_data['time_label'] = price_data['settlement_period'].apply(settlement_to_time)
        
        chart = alt.Chart(price_data).mark_line(color='#2ecc71', point=True).encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('system_sell_price:Q', title='System Sell Price (£/MWh)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('system_sell_price:Q', title='Price (£/MWh)', format=',.2f')
            ]
        ).properties(
            width=700,
            height=300,
            title='System Sell Price'
        ).interactive()
    else:
        # Multiple days - use datetime
        price_data = df.groupby('settlement_date')['system_sell_price'].first().reset_index()
        
        chart = alt.Chart(price_data).mark_line(color='#2ecc71', point=True).encode(
            x=alt.X('settlement_date:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('system_sell_price:Q', title='System Sell Price (£/MWh)'),
            tooltip=[
                alt.Tooltip('settlement_date:T', title='Date', format='%Y-%m-%d'),
                alt.Tooltip('system_sell_price:Q', title='Price (£/MWh)', format=',.2f')
            ]
        ).properties(
            width=700,
            height=300,
            title='System Sell Price'
        ).interactive()
    
    return chart


def create_demand_chart(df: pd.DataFrame) -> alt.Chart:
    """Create line chart for national and transmission demand."""
    
    # Check if demand columns exist
    if 'national_demand' not in df.columns:
        return None
    
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    if unique_dates == 1:
        # Single day - use settlement periods
        demand_data = df.groupby('settlement_period').agg({
            'national_demand': 'first',
            'transmission_system_demand': 'first'
        }).reset_index()
        
        demand_data['time_label'] = demand_data['settlement_period'].apply(settlement_to_time)
        
        demand_long = demand_data.melt(
            id_vars=['settlement_period', 'time_label'],
            value_vars=['national_demand', 'transmission_system_demand'],
            var_name='type',
            value_name='demand'
        )
        
        demand_long['type'] = demand_long['type'].map({
            'national_demand': 'National Demand',
            'transmission_system_demand': 'Transmission System Demand'
        })
        
        chart = alt.Chart(demand_long).mark_line(point=True).encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('demand:Q', title='Demand (MW)'),
            color=alt.Color('type:N', title='Demand Type'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('demand:Q', title='Demand (MW)', format=',.0f')
            ]
        ).properties(
            width=700,
            height=300,
            title='Power Demand'
        ).interactive()
    else:
        # Multiple days - use datetime
        demand_data = df.groupby('settlement_date').agg({
            'national_demand': 'first',
            'transmission_system_demand': 'first'
        }).reset_index()
        
        demand_long = demand_data.melt(
            id_vars=['settlement_date'],
            value_vars=['national_demand', 'transmission_system_demand'],
            var_name='type',
            value_name='demand'
        )
        
        demand_long['type'] = demand_long['type'].map({
            'national_demand': 'National Demand',
            'transmission_system_demand': 'Transmission System Demand'
        })
        
        chart = alt.Chart(demand_long).mark_line(point=True).encode(
            x=alt.X('settlement_date:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('demand:Q', title='Demand (MW)'),
            color=alt.Color('type:N', title='Demand Type'),
            tooltip=[
                alt.Tooltip('settlement_date:T', title='Date', format='%Y-%m-%d'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('demand:Q', title='Demand (MW)', format=',.0f')
            ]
        ).properties(
            width=700,
            height=300,
            title='Power Demand'
        ).interactive()
    
    return chart


def create_fuel_breakdown_chart(df: pd.DataFrame) -> alt.Chart:
    """Create pie chart showing fuel mix distribution."""
    
    # Check how many unique dates
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    # Get total generation by fuel type across all selected data
    fuel_mix = df.groupby('fuel_type')['generation_mw'].sum().reset_index()
    fuel_mix = fuel_mix.sort_values('generation_mw', ascending=False)
    
    # Create appropriate title
    if unique_dates == 1:
        date_str = df['settlement_date'].dt.date.iloc[0].strftime("%Y-%m-%d")
        title_str = f'Supply Distribution ({date_str})'
    else:
        start_date = df['settlement_date'].min().strftime("%Y-%m-%d")
        end_date = df['settlement_date'].max().strftime("%Y-%m-%d")
        title_str = f'Supply Distribution ({start_date} to {end_date})'
    
    chart = alt.Chart(fuel_mix).mark_arc(innerRadius=50).encode(
        theta=alt.Theta('generation_mw:Q', title='Supply (MW)'),
        color=alt.Color('fuel_type:N', title='Fuel Type', scale=alt.Scale(scheme='category20')),
        tooltip=[
            alt.Tooltip('fuel_type:N', title='Fuel Type'),
            alt.Tooltip('generation_mw:Q', title='Total Supply (MW)', format=',.0f')
        ]
    ).properties(
        width=350,
        height=350,
        title=title_str
    )
    
    return chart


def create_carbon_demand_chart(df: pd.DataFrame) -> alt.Chart:
    """Create simple dual-axis chart showing demand vs carbon intensity."""
    
    # Check if demand data is available
    if 'national_demand' not in df.columns:
        return None
    
    unique_dates = df['settlement_date'].dt.date.nunique()
    
    if unique_dates == 1:
        # Single day - use settlement periods
        data = df.groupby('settlement_period').agg({
            'intensity_actual': 'first',
            'national_demand': 'first'
        }).reset_index()
        
        data['time_label'] = data['settlement_period'].apply(settlement_to_time)
        
        # Carbon intensity line (primary axis)
        carbon_line = alt.Chart(data).mark_line(color='#e74c3c', strokeWidth=2.5, point=True).encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('intensity_actual:Q', title='Carbon Intensity (gCO2/kWh)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('intensity_actual:Q', title='Carbon Intensity', format='.1f'),
                alt.Tooltip('national_demand:Q', title='Demand (MW)', format=',.0f')
            ]
        )
        
        # Demand line (secondary axis)
        demand_line = alt.Chart(data).mark_line(color='#3498db', strokeWidth=2.5, point=True).encode(
            x=alt.X('settlement_period:Q'),
            y=alt.Y('national_demand:Q', title='National Demand (MW)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('national_demand:Q', title='Demand (MW)', format=',.0f')
            ]
        )
        
        # Combine charts
        chart = alt.layer(
            carbon_line,
            demand_line
        ).resolve_scale(
            y='independent'
        ).properties(
            width=700,
            height=400,
            title='National Demand vs Carbon Intensity'
        )
        
    else:
        # Multiple days - average across all days
        data = df.groupby('settlement_period').agg({
            'intensity_actual': 'mean',
            'national_demand': 'mean'
        }).reset_index()
        
        data['time_label'] = data['settlement_period'].apply(settlement_to_time)
        
        # Carbon intensity line
        carbon_line = alt.Chart(data).mark_line(color='#e74c3c', strokeWidth=2.5, point=True).encode(
            x=alt.X('settlement_period:Q', 
                    title='Time of Day',
                    scale=alt.Scale(domain=[1, 48]),
                    axis=alt.Axis(
                        values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                        labelExpr="datum.value == 1 ? '00:00' : datum.value == 5 ? '02:00' : datum.value == 9 ? '04:00' : datum.value == 13 ? '06:00' : datum.value == 17 ? '08:00' : datum.value == 21 ? '10:00' : datum.value == 25 ? '12:00' : datum.value == 29 ? '14:00' : datum.value == 33 ? '16:00' : datum.value == 37 ? '18:00' : datum.value == 41 ? '20:00' : datum.value == 45 ? '22:00' : '24:00'"
                    )),
            y=alt.Y('intensity_actual:Q', title='Avg Carbon Intensity (gCO2/kWh)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('intensity_actual:Q', title='Avg Carbon Intensity', format='.1f'),
                alt.Tooltip('national_demand:Q', title='Avg Demand (MW)', format=',.0f')
            ]
        )
        
        # Demand line
        demand_line = alt.Chart(data).mark_line(color='#3498db', strokeWidth=2.5, point=True).encode(
            x=alt.X('settlement_period:Q'),
            y=alt.Y('national_demand:Q', title='Avg National Demand (MW)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('national_demand:Q', title='Avg Demand (MW)', format=',.0f')
            ]
        )
        
        # Combine charts
        chart = alt.layer(
            carbon_line,
            demand_line
        ).resolve_scale(
            y='independent'
        ).properties(
            width=700,
            height=400,
            title='Average National Demand vs Carbon Intensity'
        )
    
    return chart


# Streamlit App
st.set_page_config(page_title="UK Power Supply Monitor", page_icon="⚡", layout="wide")

# Initialize connection
try:
    secrets = get_secrets()
    load_secrets_to_env(secrets)
except Exception as e:
    st.error(f"Error loading credentials: {e}")
    st.stop()

# Header
st.title("UK Power Supply Monitor")
st.markdown("Real-time and historical tracking of power supply, carbon intensity, and system pricing")

# Sidebar filters
st.sidebar.header("Filters")

time_filter = st.sidebar.radio(
    "Time Range",
    ["recent", "historical"],
    format_func=lambda x: "Recent (Last 24 hours)" if x == "recent" else "Historical (> 24 hours)"
)

# Date range for historical data
if time_filter == "historical":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.now() - timedelta(days=1))
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
        help="Choose which fuel types to include in the analysis. Charts will show only selected types."
    )
    
    if not selected_fuel_types:
        st.sidebar.warning("Select at least one fuel type")
        st.error("Please select at least one fuel type.")
        st.stop()
    
    st.sidebar.caption("Tip: Select 2-3 fuel types to compare their relative relationship")
    
    # Filter data by selected fuel types
    df = df_initial[df_initial['fuel_type'].isin(selected_fuel_types)].copy()
    
    # Show selection count
    st.sidebar.success(f"{len(selected_fuel_types)} of {len(available_fuel_types)} fuel types selected")
    
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
        
        period_data = df.groupby(['settlement_date', 'settlement_period']).agg(agg_dict).reset_index()
        
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
        fuel_stats = df.groupby('fuel_type')['generation_mw'].agg(['mean', 'max', 'min']).round(2)
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
            df.sort_values(['settlement_date', 'settlement_period'], ascending=False),
            use_container_width=True
        )
    
    # Auto-refresh for recent data
    if time_filter == "recent":
        st.sidebar.markdown("---")
        st.sidebar.info("Dashboard auto-refreshes every 5 minutes for live supply, carbon intensity, and pricing data")

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.exception(e)