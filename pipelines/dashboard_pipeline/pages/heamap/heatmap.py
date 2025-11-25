import streamlit as st
import pandas as pd
import pgeocode
import plotly.express as px
import psycopg2

# --- 1. SETUP ---
st.set_page_config(page_title="UK Power Outage Heatmap", layout="wide")
st.title("⚡ UK Power Outage Heatmap")

nomi = pgeocode.Nominatim('gb')

# --- 2. DATABASE CONNECTION ---


@st.cache_resource
def init_connection():
    try:
        return psycopg2.connect(**st.secrets["db_credentials"])
    except Exception as e:
        st.error(f"❌ DB Connection Failed: {e}")
        return None

# --- 3. DATA LOADING ---


@st.cache_data(ttl=600)
def get_outage_data():
    conn = init_connection()
    if not conn:
        return pd.DataFrame()

    # Extract Outcode (e.g. 'RM8') and count outages
    query = """
    SELECT 
        SPLIT_PART(UPPER(p.postcode_affected), ' ', 1) as postcode,
        COUNT(p.outage_id) as outage_count
    FROM 
        BRIDGE_affected_postcodes p
    JOIN 
        FACT_outage f ON p.outage_id = f.outage_id
    GROUP BY 
        1;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
    conn.close()

    df = pd.DataFrame(data, columns=colnames)
    if not df.empty:
        df['postcode'] = df['postcode'].str.upper().str.strip()
    return df


# --- 4. VISUALIZATION ---
with st.spinner('Loading data...'):
    df = get_outage_data()

if df.empty:
    st.warning("No data found.")
    st.stop()

# Geocoding
geo_data = nomi.query_postal_code(df['postcode'].tolist())
df['lat'] = geo_data.latitude
df['lon'] = geo_data.longitude
df_mapped = df.dropna(subset=['lat', 'lon'])

st.subheader("Outage Density by District")

# Get outage count range
min_outages = int(df_mapped['outage_count'].min())
max_outages = int(df_mapped['outage_count'].max())

# --- SIDEBAR CONTROLS ---
st.sidebar.header("Map Controls")

# Outage count filter
outage_range = st.sidebar.slider(
    "Filter by Outage Count",
    min_value=min_outages,
    max_value=max_outages,
    value=(min_outages, max_outages),
    step=1
)

# Bubble size control
bubble_size = st.sidebar.slider(
    "Bubble Radius",
    min_value=10,
    max_value=50,
    value=30,
    step=1
)

st.sidebar.info(f"Showing {outage_range[0]} - {outage_range[1]} outages")

# Filter data based on outage count
df_filtered = df_mapped[
    (df_mapped['outage_count'] >= outage_range[0]) &
    (df_mapped['outage_count'] <= outage_range[1])
]

# Fixed Bubble Map (Scatter Mapbox)
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
st.plotly_chart(fig, width="stretch")
