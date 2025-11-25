"""Main dashboard page for Power Monitor application."""

import streamlit as st

st.set_page_config(
    page_title="Power Monitor Dashboard",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ UK Power Monitor Dashboard")
st.markdown("""
Welcome to the UK Power Monitor Dashboard. This application provides
real-time insights into UK power generation, carbon intensity, and power
outage alerts.

## Features

- **Power Generation Tracking**: Monitor real-time UK energy generation
  by fuel type
- **Carbon Intensity**: Track the carbon footprint of UK electricity
  generation
- **Power Cut Alerts**: Subscribe to receive notifications about power
  outages in your area

## Get Started

Use the sidebar to navigate between different pages:

- **Subscription Form**: Sign up for power cut alerts in your postcode
- Add more pages as they become available

---

*Data updated regularly from UK energy providers and NESO.*
""")

st.info("Navigate to the Subscription Form page to sign up for alerts.")
