"""
Lambda function to generate AI-powered summaries of UK energy data.
Queries RDS for power cuts, generation, pricing, and carbon intensity.
Saves summaries to S3 for dashboard consumption.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
import psycopg2
from openai import OpenAI

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secretsmanager_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')

# ==============================================================================
# Get secrets from AWS Secrets Manager
def get_secret(secret_arn: str) -> Dict:
    """Retrieve a secret from AWS Secrets Manager."""
    try:
        response = secretsmanager_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response['SecretString'])
        logger.info(f"Successfully retrieved secret: {secret_arn}")
        return secret
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_arn}: {e}")
        raise


def load_secrets():
    """Load all required secrets and set as environment variables."""
    db_secret_arn = os.environ.get('DB_CREDENTIALS_SECRET_ARN')
    openai_secret_arn = os.environ.get('OPENAI_SECRET_ARN')

    if not db_secret_arn or not openai_secret_arn:
        raise ValueError("Secret ARNs not found in environment variables")

    # Load DB credentials
    db_credentials = get_secret(db_secret_arn)
    for key, value in db_credentials.items():
        os.environ[key] = str(value)

    # Load OpenAI API key
    openai_secrets = get_secret(openai_secret_arn)
    for key, value in openai_secrets.items():
        os.environ[key] = str(value)

    logger.info("All secrets loaded successfully")

# ==============================================================================
# Database Connection
def get_db_connection():
    """Create connection to PostgreSQL RDS database."""
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USERNAME'],
            password=os.environ['DB_PASSWORD'],
            port=os.environ.get('DB_PORT', '5432')
        )
        logger.info("Successfully connected to database")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


# ==============================================================================
# Data extraction for power outage

def fetch_power_outages(conn, hours: int = 24) -> Dict:
    """Fetch recent power outages from the last X hours."""
    cursor = conn.cursor()
    cutoff_time = datetime.now() - timedelta(hours=hours)

    query = """
        SELECT 
            fo.source_provider,
            fo.status,
            fo.outage_date,
            fo.region_affected,
            COUNT(bap.postcode_affected) as num_postcodes
        FROM FACT_outage fo
        LEFT JOIN BRIDGE_affected_postcodes bap ON fo.outage_id = bap.outage_id
        WHERE fo.recording_time >= %s
        GROUP BY fo.outage_id, fo.source_provider, fo.status, fo.outage_date, fo.region_affected
        ORDER BY fo.outage_date DESC
    """

    try:
        cursor.execute(query, (cutoff_time,))
        outages = []
        for row in cursor.fetchall():
            outages.append({
                'provider': row[0],
                'status': row[1],
                'date': row[2].isoformat() if row[2] else None,
                'region': row[3],
                'postcodes_affected': row[4]
            })

        # Aggregate statistics
        stats = {
            'total_outages': len(outages),
            'planned': sum(1 for o in outages if o['status'] == 'planned'),
            'unplanned': sum(1 for o in outages if o['status'] == 'unplanned'),
            'total_postcodes': sum(o['postcodes_affected'] for o in outages),
            'by_provider': {}
        }

        for outage in outages:
            provider = outage['provider']
            if provider not in stats['by_provider']:
                stats['by_provider'][provider] = {'count': 0, 'postcodes': 0}
            stats['by_provider'][provider]['count'] += 1
            stats['by_provider'][provider]['postcodes'] += outage['postcodes_affected']

        logger.info(f"Fetched {len(outages)} power outages")
        cursor.close()
        return stats

    except Exception as e:
        logger.error(f"Failed to fetch power outages: {e}")
        cursor.close()
        raise


# ==============================================================================
# Data extraction - Power Generation

def fetch_power_generation(conn, hours: int = 24) -> Dict:
    """Fetch recent power generation data by fuel type."""
    cursor = conn.cursor()
    cutoff_time = datetime.now() - timedelta(hours=hours)

    query = """
        SELECT 
            ft.fuel_type,
            SUM(g.generation_mw) as total_generation,
            AVG(g.generation_mw) as avg_generation,
            COUNT(*) as num_readings
        FROM generation g
        JOIN fuel_type ft ON g.fuel_type_id = ft.fuel_type_id
        JOIN settlements s ON g.settlement_id = s.settlement_id
        WHERE s.settlement_date >= %s
        GROUP BY ft.fuel_type
        ORDER BY total_generation DESC
    """

    try:
        cursor.execute(query, (cutoff_time,))
        generation_data = []
        total_mw = 0

        for row in cursor.fetchall():
            fuel_type, total_gen, avg_gen, readings = row
            generation_data.append({
                'fuel_type': fuel_type,
                'total_mw': float(total_gen),
                'average_mw': float(avg_gen),
                'readings': readings
            })
            total_mw += float(total_gen)

        # Calculate percentages
        for item in generation_data:
            item['percentage'] = round(
                (item['total_mw'] / total_mw * 100), 2) if total_mw > 0 else 0

        stats = {
            'total_generation_mw': round(total_mw, 2),
            'by_fuel_type': generation_data
        }

        logger.info(
            f"Fetched power generation data: {len(generation_data)} fuel types")
        cursor.close()
        return stats

    except Exception as e:
        logger.error(f"Failed to fetch power generation: {e}")
        cursor.close()
        raise


# ==============================================================================
# Data extraction - System Pricing

def fetch_system_pricing(conn, hours: int = 24) -> Dict:
    """Fetch recent system sell prices."""
    cursor = conn.cursor()
    cutoff_time = datetime.now() - timedelta(hours=hours)

    query = """
        SELECT 
            AVG(sp.system_sell_price) as avg_price,
            MIN(sp.system_sell_price) as min_price,
            MAX(sp.system_sell_price) as max_price,
            COUNT(*) as num_periods
        FROM system_price sp
        JOIN settlements s ON sp.settlement_id = s.settlement_id
        WHERE s.settlement_date >= %s
    """

    try:
        cursor.execute(query, (cutoff_time,))
        row = cursor.fetchone()

        stats = {
            'average_price': round(float(row[0]), 2) if row[0] else 0,
            'min_price': round(float(row[1]), 2) if row[1] else 0,
            'max_price': round(float(row[2]), 2) if row[2] else 0,
            'num_periods': row[3]
        }

        logger.info(f"Fetched pricing data: avg £{stats['average_price']}/MWh")
        cursor.close()
        return stats

    except Exception as e:
        logger.error(f"Failed to fetch system pricing: {e}")
        cursor.close()
        raise


# ==============================================================================
# Data extraction - Carbon Intensity

def fetch_carbon_intensity(conn, hours: int = 24) -> Dict:
    """Fetch recent carbon intensity data."""
    cursor = conn.cursor()
    cutoff_time = datetime.now() - timedelta(hours=hours)

    query = """
        SELECT 
            AVG(ci.intensity_actual) as avg_intensity,
            MIN(ci.intensity_actual) as min_intensity,
            MAX(ci.intensity_actual) as max_intensity,
            ci.intensity_index as latest_index
        FROM carbon_intensity ci
        JOIN settlements s ON ci.settlement_id = s.settlement_id
        WHERE s.settlement_date >= %s
        GROUP BY ci.intensity_index
        ORDER BY MAX(s.settlement_date) DESC
        LIMIT 1
    """

    try:
        cursor.execute(query, (cutoff_time,))
        row = cursor.fetchone()

        if row:
            stats = {
                'average_intensity': round(float(row[0]), 2) if row[0] else 0,
                'min_intensity': round(float(row[1]), 2) if row[1] else 0,
                'max_intensity': round(float(row[2]), 2) if row[2] else 0,
                'intensity_index': row[3]
            }
        else:
            stats = {
                'average_intensity': 0,
                'min_intensity': 0,
                'max_intensity': 0,
                'intensity_index': 'unknown'
            }

        logger.info(
            f"Fetched carbon intensity: avg {stats['average_intensity']} gCO2/kWh")
        cursor.close()
        return stats

    except Exception as e:
        logger.error(f"Failed to fetch carbon intensity: {e}")
        cursor.close()
        raise


# ==============================================================================
# AI Summary Generation

def generate_openai_summary(all_data: Dict) -> str:
    """Generate human-readable summary using OpenAI API."""
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OpenAI API key not found")

    client = OpenAI(api_key=api_key)

    prompt = f"""You are a UK energy analyst creating an accessible summary for the general public.

Analyze the following UK energy data from the last 24 hours:

POWER GENERATION:
- Total Generation: {all_data['generation']['total_generation_mw']} MW
- Top Sources: {json.dumps(all_data['generation']['by_fuel_type'][:3], indent=2)}

CARBON INTENSITY:
- Average: {all_data['carbon']['average_intensity']} gCO2/kWh
- Index: {all_data['carbon']['intensity_index']}
- Range: {all_data['carbon']['min_intensity']} - {all_data['carbon']['max_intensity']} gCO2/kWh

SYSTEM PRICING:
- Average Price: £{all_data['pricing']['average_price']}/MWh
- Range: £{all_data['pricing']['min_price']} - £{all_data['pricing']['max_price']}/MWh

POWER OUTAGES:
- Total Outages: {all_data['outages']['total_outages']}
- Planned: {all_data['outages']['planned']}, Unplanned: {all_data['outages']['unplanned']}
- Postcodes Affected: {all_data['outages']['total_postcodes']}

Generate a concise 3-4 paragraph summary that:
1. Highlights the overall energy situation in the UK today
2. Notes any significant trends in generation mix (renewables vs fossil fuels)
3. Discusses carbon intensity and what it means for the environment
4. Comments on pricing and any notable patterns
5. Addresses power outages if significant
6. Ends with a brief outlook or insight

Use clear, accessible language for a general audience. Be informative but engaging."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful UK energy analyst who explains complex data clearly to the general public."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=600
        )

        summary = response.choices[0].message.content
        logger.info(
            f"Generated AI summary - Tokens used: {response.usage.total_tokens}")
        return summary

    except Exception as e:
        logger.error(f"OpenAI API failed: {e}")
        return generate_fallback_summary(all_data)


def generate_fallback_summary(all_data: Dict) -> str:
    """Generate basic summary if OpenAI fails."""
    summary = f"""UK Energy Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}

Power Generation: {all_data['generation']['total_generation_mw']} MW total
Top fuel types: {', '.join([f['fuel_type'] for f in all_data['generation']['by_fuel_type'][:3]])}

Carbon Intensity: {all_data['carbon']['average_intensity']} gCO2/kWh ({all_data['carbon']['intensity_index']})

System Price: £{all_data['pricing']['average_price']}/MWh average

Power Outages: {all_data['outages']['total_outages']} total ({all_data['outages']['unplanned']} unplanned)
Postcodes affected: {all_data['outages']['total_postcodes']}
"""
    return summary


# ==============================================================================
# S3 Storage Save

def save_summary_to_s3(summary_data: Dict) -> str:
    """Save summary to S3 bucket as JSON file."""
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME not found in environment")

    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    s3_key = f"summaries/summary-{timestamp}.json"

    try:
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(summary_data, indent=2),
            ContentType='application/json'
        )

        # Also save as "latest" for easy dashboard access
        s3_client.put_object(
            Bucket=bucket_name,
            Key='summaries/summary-latest.json',
            Body=json.dumps(summary_data, indent=2),
            ContentType='application/json'
        )

        logger.info(f"Summary saved to S3: s3://{bucket_name}/{s3_key}")
        return s3_key

    except Exception as e:
        logger.error(f"Failed to save to S3: {e}")
        raise


# ==============================================================================
# Lambda Handler

def lambda_handler(event, context):
    """AWS Lambda handler - main entry point."""
    logger.info("Starting AI summary generation")
    logger.info(f"Event: {json.dumps(event)}")

    try:
        # Step 1: Load secrets
        logger.info("Loading secrets...")
        load_secrets()

        # Step 2: Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection()

        # Step 3: Fetch all data (last 24 hours)
        logger.info("Fetching data from RDS...")
        outages_data = fetch_power_outages(conn, hours=24)
        generation_data = fetch_power_generation(conn, hours=24)
        pricing_data = fetch_system_pricing(conn, hours=24)
        carbon_data = fetch_carbon_intensity(conn, hours=24)

        conn.close()
        logger.info("Database connection closed")

        # Step 4: Combine all data
        all_data = {
            'outages': outages_data,
            'generation': generation_data,
            'pricing': pricing_data,
            'carbon': carbon_data
        }

        # Step 5: Generate AI summary
        logger.info("Generating AI summary...")
        ai_summary = generate_openai_summary(all_data)

        # Step 6: Prepare summary data for S3
        summary_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': ai_summary,
            'data': all_data,
            'metadata': {
                'generated_by': 'AI Summary Lambda',
                'data_period': 'Last 24 hours'
            }
        }

        # Step 7: Save to S3
        logger.info("Saving summary to S3...")
        s3_key = save_summary_to_s3(summary_data)

        # Step 8: Log summary
        logger.info("="*80)
        logger.info("GENERATED SUMMARY:")
        logger.info(ai_summary)
        logger.info("="*80)

        # Return success
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Summary generated successfully',
                's3_key': s3_key,
                'timestamp': datetime.now().isoformat()
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }


# ==============================================================================
# Local Testing

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    result = lambda_handler({}, None)
    print(json.dumps(json.loads(result['body']), indent=2))
