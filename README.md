# Hypercubed Power Monitor

Real-time UK energy monitoring platform with power generation, outage tracking, and AI-powered insights.

## Overview

Complete data pipeline combining 6 UK distribution networks and national grid operators. Ingests power cuts and generation data into PostgreSQL, triggers alerts via email, generates AI summaries, and visualizes everything in an interactive Streamlit dashboard.

## Components

| Component | Purpose | README |
|-----------|---------|--------|
| Dashboard | Streamlit web interface with heatmaps and AI summaries | [dashboard_pipeline](pipelines/dashboard_pipeline/README.md) |
| RDS Pipeline | ETL ingestion from 6 power networks + generation data | [rds_pipeline](pipelines/rds_pipeline/README.md) |
| Alerts | SES email alerts for power outages to subscribers | [alerts_pipeline](pipelines/alerts_pipeline/README.md) |
| AI Summary | OpenAI-powered energy insights generation | [ai_summary](ai_summary/README.md) |
| Infrastructure | Terraform IaC for AWS deployment | [terraform](terraform/README.md) |

## Quick Start

```bash
# Local dashboard
cd pipelines/dashboard_pipeline/app
pip install -r requirements.txt
streamlit run app.py

# Deploy infrastructure
cd terraform
terraform apply -var-file="terraform.tfvars"
```

## Data Sources

**Outages** - National Grid, UK Power Networks, Northern Powergrid, SSE, SP Energy, NIE Networks

**Generation** - Elexon (BMRS), Carbon Intensity API, NESO grid demand

## ERD

https://drawsql.app/teams/sigma-labs-85/diagrams/subscriber-alerts

## Architecture

```
UK Grid Operators
  ├─ Power Cuts (6 networks)
  └─ Generation (Elexon, Carbon, NESO)
         ↓
   RDS Pipeline ETL (Lambda)
         ↓
   PostgreSQL Database
      ↙  ↓  ↘
Alerts   AI Summary   Dashboard
 (SES)   (OpenAI)    (Streamlit)
         ↓
      S3 Storage
```

## Environment

Requires AWS credentials and Secrets Manager configuration for:
- RDS database
- Grid operator APIs
- OpenAI API key
