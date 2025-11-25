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
def get_live_outage_data() -> pd.DataFrame:
    """Fetch live outage data from the database for the last 3 hours.

    Returns:
        pd.DataFrame: DataFrame containing postcode, provider, status, outage date,
                      recording time, and outage count."""

    conn = init_connection()
    if not conn:
        return pd.DataFrame()

    query = """
    SELECT SPLIT_PART(UPPER(bap.postcode_affected), ' ', 1) as postcode,
        fo.source_provider,
        fo.status,
        fo.outage_date,
        fo.recording_time
    FROM bridge_affected_postcodes bap
    JOIN fact_outage fo 
    ON bap.outage_id = fo.outage_id
    WHERE fo.recording_time >= NOW() - INTERVAL '3 hour'
    """

    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    df = pd.DataFrame(data, columns=cols)
    if not df.empty:
        df['outage_count'] = df.groupby(
            'postcode')['postcode'].transform('count')

    return df


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
        hover_data={'outage_count': ':.0f'},
        title="UK Power Outages by Postcode District",
        labels={'outage_count': 'Count'}
    )
    fig.update_layout(
        height=600,
        coloraxis_colorbar=dict(title='Count')
    )
    return fig
