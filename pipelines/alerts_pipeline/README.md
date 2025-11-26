# Alerts Pipeline

AWS Lambda service for sending power outage alerts to subscribed customers.

## Overview

Automated alerting system that:
- Queries RDS database for customers needing outage notifications
- Sends personalized emails via AWS SES
- Implements anti-spam logic to prevent duplicate alerts
- Logs sent notifications for audit trail

## Files

- `alerts_lambda.py` - AWS Lambda handler entry point
- `extract_alerts_from_rds.py` - Database queries and secret management
- `process_alerts.py` - Email generation and SES integration
- `requirements.txt` - Python dependencies

## Deployment

```bash
# Build Lambda Docker image
bash bash_scripts/create_alerts_lambda_image.sh

# Push to ECR
bash bash_scripts/push_alerts_lambda_image.sh

# Update existing Lambda
bash bash_scripts/update_alerts_lambda_image.sh
```

## Tech Stack

- **AWS Lambda**: Serverless compute
- **RDS**: Customer and notification data
- **SES**: Email delivery
- **Psycopg2**: PostgreSQL driver

## Environment

Requires AWS credentials and RDS connection details via AWS Secrets Manager.
