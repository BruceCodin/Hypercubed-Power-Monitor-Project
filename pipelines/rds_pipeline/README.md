# RDS Pipeline

Data ingestion pipelines for power generation and outage data into PostgreSQL RDS.

## Overview

Extracts, transforms, and loads energy data from multiple UK grid operators:

**Power Generation Data** (30-minute intervals)
- Carbon intensity (Carbon API)
- Settlement data (Elexon/BMRS)
- Grid demand (NESO)

**Power Outage Data** (real-time)
- National Grid
- UK Power Networks
- Northern Powergrid
- SSE Networks
- SP Energy
- SP Northwest
- NIE Networks

## Structure

```
db_schema/              # PostgreSQL database schemas
    power_generation_schema.sql    # Settlements, pricing, carbon, demand
    subscriber_alerts_schema.sql   # Outages, customers, subscriptions
    initialise_rds.bash

power_cuts/             # Outage data pipelines (Lambda)
    [network]_pipeline/extract_*.py, transform_*.py
    transform_script/   # Common outage transformation
    lambda_handler_power_cuts.py

power_generation/       # Generation data pipelines (Lambda)
    carbon_pipeline/
    elexon_pipeline/
    neso_pipeline/
```

## Quick Start

```bash
# Initialize database
bash db_schema/initialise_rds.bash

# Build Lambda images
bash power_cuts/bash_scripts/create_power_cuts_lambda_image.sh
bash power_generation/bash_scripts/create_generation_lambda_image.sh

# Deploy to ECR
bash */bash_scripts/push_*_image.sh
```

## Tech Stack

- **PostgreSQL**: RDS database
- **AWS Lambda**: Scheduled ETL runners
- **Boto3**: AWS integration
- **Psycopg2**: PostgreSQL driver

## Environment

Requires AWS credentials and RDS endpoint via environment variables or AWS Secrets Manager.
