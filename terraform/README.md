# Terraform Infrastructure

Infrastructure as Code for deploying Hypercubed Power Monitor to AWS.

## Overview

Modular Terraform configuration for complete system deployment:
- **RDS** - PostgreSQL database for power data
- **ECR** - Container registries for Lambda and Streamlit images
- **Lambda** - Scheduled data ingestion and processing
- **Streamlit** - Dashboard web server
- **EventBridge** - Job scheduling and orchestration
- **S3** - Summary storage
- **Secrets Manager** - API keys and credentials

## Module Structure

```
powercut-etl/        # Power cuts data pipelines (6 networks)
30min_etl/           # Generation data (30-minute cadence)
neso-etl/            # Grid demand pipeline
s3-etl/              # S3 ETL jobs
alerts-etl/          # Customer alert system
ai-summaries/        # AI summary generation
dashboard/           # Streamlit dashboard deployment

terraform_secrets/   # Secrets Manager configuration
templates/           # Reusable component templates
```

## Quick Start

```bash
# Initialize Terraform
terraform init

# Review deployment plan
terraform plan -var-file="terraform.tfvars"

# Apply configuration
terraform apply -var-file="terraform.tfvars"
```

## Required Variables

See each module's `variables.tf` for configuration. Common variables:
- `aws_region` - AWS region
- `environment` - Deployment environment (dev/prod)
- Database credentials and API keys

## Provider

- **Terraform** >= 1.0
- **AWS Provider** ~> 5.0
