import pgeocode
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import os
from dotenv import load_dotenv

# --- 1. DATABASE CONNECTION ---

load_dotenv()  # Load environment variables from .env file


@st.cache_resource
def init_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("host"),
            database=os.getenv("dbname"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            port=os.getenv("port")
        )
        return conn
    except Exception as e:
        st.error(f"❌ DB Connection Failed: {e}")
        return None


# --- 2. DATA LOADING ---


@st.cache_data(ttl=300)
def get_outage_data():
    conn = init_connection()
    if not conn:
        return pd.DataFrame()

    # Query 1: Heatmap Data (Postcode Districts) from last 24 hours
    query_map = """
    SELECT 
        SPLIT_PART(UPPER(p.postcode_affected), ' ', 1) as postcode,
        COUNT(p.outage_id) as outage_count
    FROM 
        BRIDGE_affected_postcodes p
    JOIN 
        FACT_outage f ON p.outage_id = f.outage_id
    WHERE 
    	f.recording_time >= NOW() - INTERVAL '24 hours'
    GROUP BY 
        1;
    """

    # Query 2: KPI Metrics (Total Outages, Top Provider, Main Status)
    query_kpis = """
    SELECT 
        (SELECT COUNT(*) FROM FACT_outage) as total_outages,
        (SELECT source_provider FROM FACT_outage GROUP BY source_provider ORDER BY COUNT(*) DESC LIMIT 1) as top_provider,
        (SELECT status FROM FACT_outage GROUP BY status ORDER BY COUNT(*) DESC LIMIT 1) as common_status;
    """

    with conn.cursor() as cur:
        # Fetch Map Data
        cur.execute(query_map)
        data_map = cur.fetchall()
        cols_map = [desc[0] for desc in cur.description]

        # Fetch KPI Data
        cur.execute(query_kpis)
        data_kpi = cur.fetchone()

    df = pd.DataFrame(data_map, columns=cols_map)
    if not df.empty:
        df['postcode'] = df['postcode'].str.upper().str.strip()

    return df, data_kpi


def get_mapped_df(df: pd.DataFrame) -> pd.DataFrame:

    nomi = pgeocode.Nominatim('gb')

    geo_data = nomi.query_postal_code(df['postcode'].tolist())
    df['lat'] = geo_data.latitude
    df['lon'] = geo_data.longitude
    df_mapped = df.dropna(subset=['lat', 'lon'])
    return df_mapped


def create_bubble_map(df_filtered, bubble_size) -> px.scatter_map:
    fig = px.scatter_map(
        df_filtered,
        lat='lat',
        lon='lon',
        size='outage_count',
        color='outage_count',
        color_continuous_scale='Bluered',
        size_max=bubble_size,
        center=dict(lat=54.5, lon=-2.5),
        zoom=5,
        map_style="carto-positron",
        hover_name='postcode',
        hover_data=['outage_count']
    )
    fig.update_layout(
        height=800
    )
    return fig
