import streamlit as st
from heatmap_helper import get_live_outage_data, get_mapped_df, create_bubble_map

# --- 1. SETUP ---
st.set_page_config(page_title="UK Power Outage Heatmap", layout="wide")
st.title("⚡ UK Power Outage Heatmap")
st.text(
    "Showing last 3 hours worth of live outage data"
)


# --- 2. VISUALIZATION ---
with st.spinner('Fetching live stats...'):
    df = get_live_outage_data()

if df.empty:
    st.warning("No data found.")
    st.stop()

# --- KPI DISPLAY ---
# kpis index: 0=Total, 1=Provider, 2=Status
st.divider()
# --- END KPI DISPLAY ---

# --- SIDEBAR CONTROLS ---
MIN_OUTAGES = 0
MAX_OUTAGES = 1000

st.sidebar.header("Map Controls")

# Let users filter by outage count
outage_range = st.sidebar.slider(
    "Count Filter",
    min_value=MIN_OUTAGES,
    max_value=MAX_OUTAGES,
    value=(MIN_OUTAGES, MAX_OUTAGES),
    step=1
)

# Let users adjust bubble size
bubble_size = st.sidebar.slider(
    "Bubble Radius",
    min_value=10,
    max_value=50,
    value=30,
    step=1
)
# --- END SIDEBAR CONTROLS ---


# --- BUBBLE MAP ---
df_mapped = get_mapped_df(df)

# Filter dataframe based on user selection
df_filtered = df_mapped[
    (df_mapped['outage_count'] >= outage_range[0]) &
    (df_mapped['outage_count'] <= outage_range[1])
]

# Create and display bubble map
fig = create_bubble_map(df_filtered, bubble_size)
st.plotly_chart(fig)
# -------------------
