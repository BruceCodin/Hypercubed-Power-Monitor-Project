# AI Summary

AWS Lambda service generating AI-powered energy market insights.

## Overview

Automated summary generation that:
- Queries RDS for power cuts, generation, pricing, and carbon intensity data
- Uses OpenAI API to generate natural language insights
- Stores summaries in S3 for dashboard consumption
- Runs on a scheduled trigger

## Files

- `generate_ai_summary.py` - AWS Lambda handler
- `requirements.txt` - Python dependencies
- `bash_scripts/` - Deployment automation

## Deployment

```bash
# Deploy to AWS Lambda
bash bash_scripts/deploy.sh

# Update existing Lambda function
bash bash_scripts/update.sh
```

## Tech Stack

- **AWS Lambda**: Serverless compute
- **RDS**: Energy data queries
- **S3**: Summary storage
- **OpenAI API**: Text generation
- **Boto3**: AWS integration

## Environment

Requires:
- `DB_CREDENTIALS_SECRET_ARN` - RDS database credentials in AWS Secrets Manager
- `OPENAI_SECRET_ARN` - OpenAI API key in AWS Secrets Manager
