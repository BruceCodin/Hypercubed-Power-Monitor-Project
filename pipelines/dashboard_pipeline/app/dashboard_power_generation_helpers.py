'''Functions for dashboard power generation visualizations.'''
import json
from datetime import datetime
import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import boto3

SECRETS_ARN = (
    "arn:aws:secretsmanager:eu-west-2:129033205317:"
    "secret:c20-power-monitor-db-credentials-TAc5Xx"
)


def format_fuel_type(fuel_type: str) -> str:
    """Format fuel type labels: keep abbreviations capitals, convert words to title case.

    Args:
        fuel_type (str): Raw fuel type from database

    Returns:
        str: Formatted fuel type label
    """
    # Dictionary of known abbreviations that should stay uppercase
    abbreviations = {'CCGT', 'OCGT', 'RoES', 'PS', 'NPSHYD'}

    fuel_upper = fuel_type.upper().strip()

    # If it's in our abbreviations list, keep it uppercase
    if fuel_upper in abbreviations:
        return fuel_upper

    # Otherwise, convert to title case (capitalize first letter of each word)
    return fuel_type.title()


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager."""
    client = boto3.client('secretsmanager', region_name='eu-west-2')
    response = client.get_secret_value(SecretId=SECRETS_ARN)
    secret = response['SecretString']
    return json.loads(secret)


def connect_to_database(secrets: dict) -> psycopg2.extensions.connection:
    """Connect to AWS Postgres database using provided credentials."""
    return psycopg2.connect(
        host=secrets["DB_HOST"],
        database=secrets["DB_NAME"],
        user=secrets["DB_USER"],
        password=secrets["DB_PASSWORD"],
        port=int(secrets["DB_PORT"]),
    )


@st.cache_data(ttl=300)
def get_power_data(filter_type: str, start: datetime,
                   end: datetime, secrets: dict) -> pd.DataFrame:
    """Fetch power generation, carbon intensity, and price data."""

    # Always use recent_demand table, but only join for historical data
    include_demand = filter_type == "historical"

    # Determine date filter
    if filter_type == "recent":
        date_condition = (
            "s.settlement_date >= NOW() - INTERVAL '24 hours'"
        )
    else:
        date_condition = (
            f"s.settlement_date >= '{start}' AND "
            f"s.settlement_date < '{end}'::date + INTERVAL '1 day'"
        )

    # Build query with conditional demand join
    demand_join = ""
    demand_columns = ""
    if include_demand:
        demand_join = (
            "INNER JOIN recent_demand d ON s.settlement_id = d.settlement_id"
        )
        demand_columns = (
            ",\n        d.national_demand,\n"
            "        d.transmission_system_demand"
        )

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

    conn = connect_to_database(secrets)
    data = pd.read_sql_query(query, conn)
    conn.close()

    data['settlement_date'] = pd.to_datetime(data['settlement_date'])

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

    data['fuel_type'] = data['fuel_type'].replace(interconnector_mapping)

    # Format fuel type labels (keep abbreviations uppercase, title case for words)
    data['fuel_type'] = data['fuel_type'].apply(format_fuel_type)

    return data


def settlement_to_time(period: int) -> str:
    """Convert settlement period (1-48) to time string."""
    hours = (period - 1) // 2
    minutes = ((period - 1) % 2) * 30
    return f"{hours:02d}:{minutes:02d}"


def create_generation_chart(data: pd.DataFrame) -> alt.Chart:
    """Create stacked area chart for power supply by fuel type."""

    # Check if data spans multiple days
    unique_dates = data['settlement_date'].dt.date.nunique()

    if unique_dates == 1:
        # Single day - use settlement periods
        gen_data = data.groupby(
            ['settlement_period', 'fuel_type']
        )['generation_mw'].sum().reset_index()
        gen_data['time_label'] = gen_data['settlement_period'].apply(
            settlement_to_time
        )

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        chart = alt.Chart(gen_data).mark_area().encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
            y=alt.Y('generation_mw:Q', title='Supply (MW)', stack='zero'),
            color=alt.Color(
                'fuel_type:N',
                title='Fuel Type',
                scale=alt.Scale(scheme='category20')
            ),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('fuel_type:N', title='Fuel Type'),
                alt.Tooltip(
                    'generation_mw:Q',
                    title='Supply (MW)',
                    format=',.0f'
                )
            ]
        ).properties(
            width=700,
            height=400,
            title='Power Supply by Source'
        ).interactive()
    else:
        # Multiple days - use datetime
        gen_data = data.groupby(
            ['settlement_date', 'fuel_type']
        )['generation_mw'].sum().reset_index()

        chart = alt.Chart(gen_data).mark_area().encode(
            x=alt.X(
                'settlement_date:T',
                title='Date',
                axis=alt.Axis(format='%b %d')
            ),
            y=alt.Y('generation_mw:Q', title='Supply (MW)', stack='zero'),
            color=alt.Color(
                'fuel_type:N',
                title='Fuel Type',
                scale=alt.Scale(scheme='category20')
            ),
            tooltip=[
                alt.Tooltip(
                    'settlement_date:T',
                    title='Date',
                    format='%Y-%m-%d'
                ),
                alt.Tooltip('fuel_type:N', title='Fuel Type'),
                alt.Tooltip(
                    'generation_mw:Q',
                    title='Supply (MW)',
                    format=',.0f'
                )
            ]
        ).properties(
            width=700,
            height=400,
            title='Power Supply by Source (Multi-Day View)'
        ).interactive()

    return chart


def create_carbon_intensity_chart(data: pd.DataFrame) -> alt.Chart:
    """Create line chart for carbon intensity actual vs forecast."""

    unique_dates = data['settlement_date'].dt.date.nunique()

    if unique_dates == 1:
        # Single day - use settlement periods
        carbon_data = data.groupby('settlement_period').agg({
            'intensity_actual': 'first',
            'intensity_forecast': 'first'
        }).reset_index()

        carbon_data['time_label'] = carbon_data['settlement_period'].apply(
            settlement_to_time
        )

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

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        chart = alt.Chart(carbon_long).mark_line(point=True).encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
            y=alt.Y('intensity:Q', title='Carbon Intensity (gCO2/kWh)'),
            color=alt.Color(
                'type:N',
                title='Type',
                scale=alt.Scale(
                    domain=['Actual', 'Forecast'],
                    range=['#e74c3c', '#3498db']
                )
            ),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip(
                    'intensity:Q',
                    title='Intensity (gCO2/kWh)',
                    format='.2f'
                )
            ]
        ).properties(
            width=700,
            height=300,
            title='Carbon Intensity: Actual vs Forecast'
        ).interactive()
    else:
        # Multiple days - use datetime
        carbon_data = data.groupby('settlement_date').agg({
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
            x=alt.X(
                'settlement_date:T',
                title='Date',
                axis=alt.Axis(format='%b %d')
            ),
            y=alt.Y('intensity:Q', title='Carbon Intensity (gCO2/kWh)'),
            color=alt.Color(
                'type:N',
                title='Type',
                scale=alt.Scale(
                    domain=['Actual', 'Forecast'],
                    range=['#e74c3c', '#3498db']
                )
            ),
            tooltip=[
                alt.Tooltip(
                    'settlement_date:T',
                    title='Date',
                    format='%Y-%m-%d'
                ),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip(
                    'intensity:Q',
                    title='Intensity (gCO2/kWh)',
                    format='.2f'
                )
            ]
        ).properties(
            width=700,
            height=300,
            title='Carbon Intensity: Actual vs Forecast'
        ).interactive()

    return chart


def create_price_chart(data: pd.DataFrame) -> alt.Chart:
    """Create line chart for system sell price."""

    unique_dates = data['settlement_date'].dt.date.nunique()

    if unique_dates == 1:
        # Single day - use settlement periods
        price_data = data.groupby(
            'settlement_period'
        )['system_sell_price'].first().reset_index()
        price_data['time_label'] = price_data['settlement_period'].apply(
            settlement_to_time
        )

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        chart = alt.Chart(price_data).mark_line(
            color='#2ecc71',
            point=True
        ).encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
            y=alt.Y('system_sell_price:Q', title='System Sell Price (£/MWh)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip(
                    'system_sell_price:Q',
                    title='Price (£/MWh)',
                    format=',.2f'
                )
            ]
        ).properties(
            width=700,
            height=300,
            title='System Sell Price'
        ).interactive()
    else:
        # Multiple days - use datetime
        price_data = data.groupby(
            'settlement_date'
        )['system_sell_price'].first().reset_index()

        chart = alt.Chart(price_data).mark_line(
            color='#2ecc71',
            point=True
        ).encode(
            x=alt.X(
                'settlement_date:T',
                title='Date',
                axis=alt.Axis(format='%b %d')
            ),
            y=alt.Y('system_sell_price:Q', title='System Sell Price (£/MWh)'),
            tooltip=[
                alt.Tooltip(
                    'settlement_date:T',
                    title='Date',
                    format='%Y-%m-%d'
                ),
                alt.Tooltip(
                    'system_sell_price:Q',
                    title='Price (£/MWh)',
                    format=',.2f'
                )
            ]
        ).properties(
            width=700,
            height=300,
            title='System Sell Price'
        ).interactive()

    return chart


def create_demand_chart(data: pd.DataFrame) -> alt.Chart:
    """Create line chart for national and transmission demand."""

    # Check if demand columns exist
    if 'national_demand' not in data.columns:
        return None

    unique_dates = data['settlement_date'].dt.date.nunique()

    if unique_dates == 1:
        # Single day - use settlement periods
        demand_data = data.groupby('settlement_period').agg({
            'national_demand': 'first',
            'transmission_system_demand': 'first'
        }).reset_index()

        demand_data['time_label'] = demand_data['settlement_period'].apply(
            settlement_to_time
        )

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

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        chart = alt.Chart(demand_long).mark_line(point=True).encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
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
        demand_data = data.groupby('settlement_date').agg({
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
            x=alt.X(
                'settlement_date:T',
                title='Date',
                axis=alt.Axis(format='%b %d')
            ),
            y=alt.Y('demand:Q', title='Demand (MW)'),
            color=alt.Color('type:N', title='Demand Type'),
            tooltip=[
                alt.Tooltip(
                    'settlement_date:T',
                    title='Date',
                    format='%Y-%m-%d'
                ),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('demand:Q', title='Demand (MW)', format=',.0f')
            ]
        ).properties(
            width=700,
            height=300,
            title='Power Demand'
        ).interactive()

    return chart


def create_fuel_breakdown_chart(data: pd.DataFrame) -> alt.Chart:
    """Create pie chart showing fuel mix distribution."""

    # Check how many unique dates
    unique_dates = data['settlement_date'].dt.date.nunique()

    # Get total generation by fuel type across all selected data
    fuel_mix = data.groupby(
        'fuel_type'
    )['generation_mw'].sum().reset_index()
    fuel_mix = fuel_mix.sort_values('generation_mw', ascending=False)

    # Create appropriate title
    if unique_dates == 1:
        date_str = data['settlement_date'].dt.date.iloc[0].strftime(
            "%Y-%m-%d"
        )
        title_str = f'Supply Distribution ({date_str})'
    else:
        start_str = data['settlement_date'].min().strftime("%Y-%m-%d")
        end_str = data['settlement_date'].max().strftime("%Y-%m-%d")
        title_str = f'Supply Distribution ({start_str} to {end_str})'

    chart = alt.Chart(fuel_mix).mark_arc(innerRadius=50).encode(
        theta=alt.Theta('generation_mw:Q', title='Supply (MW)'),
        color=alt.Color(
            'fuel_type:N',
            title='Fuel Type',
            scale=alt.Scale(scheme='category20')
        ),
        tooltip=[
            alt.Tooltip('fuel_type:N', title='Fuel Type'),
            alt.Tooltip(
                'generation_mw:Q',
                title='Total Supply (MW)',
                format=',.0f'
            )
        ]
    ).properties(
        width=350,
        height=350,
        title=title_str
    )

    return chart


def create_carbon_demand_chart(data: pd.DataFrame) -> alt.Chart:
    """Create simple dual-axis chart showing demand vs carbon intensity."""

    # Check if demand data is available
    if 'national_demand' not in data.columns:
        return None

    unique_dates = data['settlement_date'].dt.date.nunique()

    if unique_dates == 1:
        # Single day - use settlement periods
        chart_data = data.groupby('settlement_period').agg({
            'intensity_actual': 'first',
            'national_demand': 'first'
        }).reset_index()

        chart_data['time_label'] = chart_data['settlement_period'].apply(
            settlement_to_time
        )

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        # Carbon intensity line (primary axis)
        carbon_line = alt.Chart(chart_data).mark_line(
            color='#e74c3c',
            strokeWidth=2.5,
            point=True
        ).encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
            y=alt.Y('intensity_actual:Q', title='Carbon Intensity (gCO2/kWh)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip(
                    'intensity_actual:Q',
                    title='Carbon Intensity',
                    format='.1f'
                ),
                alt.Tooltip(
                    'national_demand:Q',
                    title='Demand (MW)',
                    format=',.0f'
                )
            ]
        )

        # Demand line (secondary axis)
        demand_line = alt.Chart(chart_data).mark_line(
            color='#3498db',
            strokeWidth=2.5,
            point=True
        ).encode(
            x=alt.X('settlement_period:Q'),
            y=alt.Y('national_demand:Q', title='National Demand (MW)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip(
                    'national_demand:Q',
                    title='Demand (MW)',
                    format=',.0f'
                )
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
        chart_data = data.groupby('settlement_period').agg({
            'intensity_actual': 'mean',
            'national_demand': 'mean'
        }).reset_index()

        chart_data['time_label'] = chart_data['settlement_period'].apply(
            settlement_to_time
        )

        label_expr = (
            "datum.value == 1 ? '00:00' : "
            "datum.value == 5 ? '02:00' : "
            "datum.value == 9 ? '04:00' : "
            "datum.value == 13 ? '06:00' : "
            "datum.value == 17 ? '08:00' : "
            "datum.value == 21 ? '10:00' : "
            "datum.value == 25 ? '12:00' : "
            "datum.value == 29 ? '14:00' : "
            "datum.value == 33 ? '16:00' : "
            "datum.value == 37 ? '18:00' : "
            "datum.value == 41 ? '20:00' : "
            "datum.value == 45 ? '22:00' : '24:00'"
        )

        # Carbon intensity line
        carbon_line = alt.Chart(chart_data).mark_line(
            color='#e74c3c',
            strokeWidth=2.5,
            point=True
        ).encode(
            x=alt.X(
                'settlement_period:Q',
                title='Time of Day',
                scale=alt.Scale(domain=[1, 48]),
                axis=alt.Axis(
                    values=[1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 48],
                    labelExpr=label_expr
                )
            ),
            y=alt.Y(
                'intensity_actual:Q',
                title='Avg Carbon Intensity (gCO2/kWh)'
            ),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip(
                    'intensity_actual:Q',
                    title='Avg Carbon Intensity',
                    format='.1f'
                ),
                alt.Tooltip(
                    'national_demand:Q',
                    title='Avg Demand (MW)',
                    format=',.0f'
                )
            ]
        )

        # Demand line
        demand_line = alt.Chart(chart_data).mark_line(
            color='#3498db',
            strokeWidth=2.5,
            point=True
        ).encode(
            x=alt.X('settlement_period:Q'),
            y=alt.Y('national_demand:Q', title='Avg National Demand (MW)'),
            tooltip=[
                alt.Tooltip('time_label:N', title='Time'),
                alt.Tooltip(
                    'national_demand:Q',
                    title='Avg Demand (MW)',
                    format=',.0f'
                )
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
