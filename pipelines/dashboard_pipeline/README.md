# Dashboard Pipeline

Streamlit-based web dashboard for monitoring UK power generation data.

## Overview

Interactive dashboard with multiple pages displaying real-time power metrics:
- **Heatmap**: Geographic heat maps of power data
- **Power Generation**: Time-series visualizations and trends
- **AI Summaries**: Generated insights from power data
- **Customer Pipeline**: Form-based customer data management

## Structure

```
app/                    # Main Streamlit application
    app.py             # Entry point with navigation
    heatmap.py         # Heatmap page
    dashboard_power_generation.py  # Power generation page
    summaries_page.py  # AI summaries display
    requirements.txt

pages/                  # Modular page components
    heatmap/
    power_generation/
    ai_summaries_page/
    customer_pipeline/  # Customer ETL forms & tests

bash_scripts/          # Docker & deployment scripts
```

## Running

```bash
# Local development
streamlit run app/app.py

# Docker
docker build -t power-dashboard .
docker run -p 8501:8501 power-dashboard
```

## Tech Stack

- **Streamlit**: UI framework
- **Plotly/Altair**: Data visualization
- **Pandas**: Data processing
- **Boto3**: AWS integration
- **PostgreSQL**: Database backend

## Environment

Create `.env` file with AWS credentials and database connection details. See `pages/customer_pipeline/.env` for example.
