import pgeocode
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import boto3
import json

# --- 1. DATABASE CONNECTION ---

SECRETS_ARN = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager.

    Returns:
        dict: Dictionary containing database credentials
    """

    client = boto3.client('secretsmanager')

    response = client.get_secret_value(
        SecretId=SECRETS_ARN
    )

    # Decrypts secret using the associated KMS key.
    secret = response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict


@st.cache_resource
def init_connection(secrets: dict) -> psycopg2.extensions.connection:
    """Connects to AWS Postgres database using Secrets Manager credentials.

    Args:
        secrets (dict): Dictionary containing database credentials

    Returns:
        psycopg2 connection object
    """

    try:
        conn = psycopg2.connect(
            host=secrets["DB_HOST"],
            database=secrets["DB_NAME"],
            user=secrets["DB_USER"],
            password=secrets["DB_PASSWORD"],
            port=int(secrets["DB_PORT"]),
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

    secrets = get_secrets()
    conn = init_connection(secrets)
    if not conn:
        return pd.DataFrame()

    query = """
    SELECT bap.postcode_affected as postcode,
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


def count_outage_status(df: pd.DataFrame) -> dict:
    """Count planned and unplanned outages from the dataframe.

    Args:
        df: DataFrame containing outage data with 'status' column

    Returns:
        dict: Dictionary with 'planned' and 'unplanned' counts
    """
    if df.empty:
        return {'planned': 0, 'unplanned': 0}

    status_counts = df['status'].value_counts().to_dict()

    return {
        'planned': status_counts.get('planned', 0),
        'unplanned': status_counts.get('unplanned', 0)
    }


def get_mapped_df(df: pd.DataFrame) -> pd.DataFrame:

    nomi = pgeocode.Nominatim('gb')

    geo_data = nomi.query_postal_code(df['postcode'].tolist())
    df['lat'] = geo_data.latitude
    df['lon'] = geo_data.longitude
    df['region'] = df['postcode'].str.split().str[0]
    df_mapped = df.dropna(subset=['lat', 'lon'])
    return df_mapped


def get_unique_regions(df: pd.DataFrame) -> list:
    """Extract unique postcode regions from dataframe.

    Args:
        df: DataFrame containing 'postcode' column

    Returns:
        list: Sorted list of unique regions (e.g., ['RM8', 'E1', 'N1', ...])
    """
    if 'region' not in df.columns:
        df['region'] = df['postcode'].str.split().str[0]
    return sorted(df['region'].unique().tolist())


def create_bubble_map(df_filtered, bubble_size) -> px.scatter_map:
    fig = px.scatter_map(
        df_filtered,
        lat='lat',
        lon='lon',
        size='outage_count',
        color='outage_count',
        color_continuous_scale='aggrnyl',
        size_max=bubble_size,
        center=dict(lat=54.5, lon=-2.5),
        zoom=5,
        map_style="carto-positron",
        hover_name='postcode',
        hover_data=['outage_count'],
        title="UK Power Outages by Postcode",
        labels={'outage_count': 'Count'}
    )
    fig.update_layout(
        height=600
    )
    return fig
