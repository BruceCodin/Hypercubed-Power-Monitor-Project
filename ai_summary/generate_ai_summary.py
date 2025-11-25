"""
Lambda function to generate AI-powered summaries of UK energy data.
Queries RDS for power cuts, generation, pricing, and carbon intensity.
Saves summaries to S3 for dashboard consumption.
"""
# pylint: disable = W1203, W1309, W0612, C0301, W0621, W0718

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict
import boto3
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
    """Retrieve a secret from AWS Secrets Manager.
    
    Args:
        secret_arn (str): The ARN of the secret to retrieve.
        
    Returns:
        Dict: The secret value as a dictionary.
    """
    response = secretsmanager_client.get_secret_value(SecretId=secret_arn)
    return json.loads(response['SecretString'])


def load_secrets():
    """Load all required secrets and set as environment variables."""
    db_secret_arn = os.environ['DB_CREDENTIALS_SECRET_ARN']  # Gets these from lambda
    openai_secret_arn = os.environ['OPENAI_SECRET_ARN']

    # Load DB credentials and OpenAI API key
    for secret in [get_secret(db_secret_arn), get_secret(openai_secret_arn)]:
        for key, value in secret.items():
            os.environ[key] = str(value)

    logger.info("Secrets loaded")


# ==============================================================================
# Database Connection
def get_db_connection():
    """Create connection to PostgreSQL RDS database."""
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=os.environ.get('DB_PORT', '5432')
    )
    logger.info("Database connected")
    return conn


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
            COUNT(bap.postcode_affected) as num_postcodes
        FROM FACT_outage fo
        LEFT JOIN BRIDGE_affected_postcodes bap ON fo.outage_id = bap.outage_id
        WHERE fo.recording_time >= %s
        GROUP BY fo.outage_id, fo.source_provider, fo.status, fo.outage_date
        ORDER BY fo.outage_date DESC
    """

    cursor.execute(query, (cutoff_time,))
    outages = []
    for row in cursor.fetchall():
        outages.append({
            'provider': row[0],
            'status': row[1],
            'date': row[2].isoformat() if row[2] else None,
            'postcodes_affected': row[3]
        })

    # Aggregate statistics
    stats = {
        'total_outages': len(outages),
        'planned': sum(1 for o in outages if o['status'] and 'planned' in o['status'].lower()),
        'unplanned': sum(1 for o in outages if o['status'] and 'unplanned' in o['status'].lower()),
        'total_postcodes': sum(o['postcodes_affected'] for o in outages),
        'by_provider': {}
    }

    for outage in outages:
        provider = outage['provider']
        if provider not in stats['by_provider']:
            stats['by_provider'][provider] = {'count': 0, 'postcodes': 0}
        stats['by_provider'][provider]['count'] += 1
        stats['by_provider'][provider]['postcodes'] += outage['postcodes_affected']

    logger.info(f"Fetched {len(outages)} outages")
    cursor.close()
    return stats


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

    logger.info(f"Fetched {len(generation_data)} fuel types")
    cursor.close()
    return {
        'total_generation_mw': round(total_mw, 2),
        'by_fuel_type': generation_data
    }


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

    cursor.execute(query, (cutoff_time,))
    row = cursor.fetchone()

    logger.info(
        f"Fetched pricing: avg £{round(float(row[0]), 2) if row[0] else 0}/MWh")
    cursor.close()

    return {
        'average_price': round(float(row[0]), 2) if row[0] else 0,
        'min_price': round(float(row[1]), 2) if row[1] else 0,
        'max_price': round(float(row[2]), 2) if row[2] else 0,
        'num_periods': row[3]
    }


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

    logger.info(f"Fetched carbon: {stats['average_intensity']} gCO2/kWh")
    cursor.close()
    return stats


# ==============================================================================
# AI Summary Generation
def generate_openai_summary(all_data: Dict) -> str:
    """Generate human-readable summary using OpenAI API."""
    client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

    prompt = f"""You are a UK energy analyst creating an accessible summary for the general public.

    Analyze the following UK energy data from the last 6 hours and generate a concise summary:

    POWER GENERATION:
    - Total: {all_data['generation']['total_generation_mw']} MW
    - Top Sources: {json.dumps(all_data['generation']['by_fuel_type'][:3], indent=2)}

    CARBON INTENSITY:
    - Average: {all_data['carbon']['average_intensity']} gCO2/kWh ({all_data['carbon']['intensity_index']})
    - Range: {all_data['carbon']['min_intensity']} - {all_data['carbon']['max_intensity']} gCO2/kWh

    PRICING:
    - Average: £{all_data['pricing']['average_price']}/MWh
    - Range: £{all_data['pricing']['min_price']} - £{all_data['pricing']['max_price']}/MWh

    OUTAGES:
    - Total: {all_data['outages']['total_outages']} (Planned: {all_data['outages']['planned']}, Unplanned: {all_data['outages']['unplanned']})
    - Postcodes Affected: {all_data['outages']['total_postcodes']}

    Generate a 1-2 paragraphs summary covering: energy mix, carbon levels, pricing patterns, and outages. 
    Use clear language for the general public."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a UK energy analyst explaining data to the public."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=600
        )

        logger.info(
            f"AI summary generated ({response.usage.total_tokens} tokens)")
        return response.choices[0].message.content

    except Exception as e:
        logger.error(f"OpenAI failed: {e}")
        return generate_fallback_summary(all_data)


def generate_fallback_summary(all_data: Dict) -> str:
    """Generate basic summary if OpenAI fails."""
    return f"""UK Energy Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}

Generation: {all_data['generation']['total_generation_mw']} MW
Carbon: {all_data['carbon']['average_intensity']} gCO2/kWh ({all_data['carbon']['intensity_index']})
Price: £{all_data['pricing']['average_price']}/MWh
Outages: {all_data['outages']['total_outages']} ({all_data['outages']['unplanned']} unplanned)"""


# ==============================================================================
# S3 Storage Save
def save_summary_to_s3(summary_data: Dict) -> str:
    """Save summary to S3 bucket as JSON file."""
    bucket_name = os.environ['S3_BUCKET_NAME']
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    s3_key = f"summaries/summary-{timestamp}.json"

    body = json.dumps(summary_data, indent=2)

    # Save timestamped version
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=body,
        ContentType='application/json'
    )

    # Also save as "latest" for easy dashboard access
    s3_client.put_object(
        Bucket=bucket_name,
        Key='summaries/summary-latest.json',
        Body=body,
        ContentType='application/json'
    )

    logger.info(f"Saved to S3: {s3_key}")
    return s3_key


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
        try:
            
            logger.info("Fetching data from RDS...")
            outages_data = fetch_power_outages(conn, hours=24)
            generation_data = fetch_power_generation(conn, hours=24)
            pricing_data = fetch_system_pricing(conn, hours=24)
            carbon_data = fetch_carbon_intensity(conn, hours=24)
        finally: # Always close connection if anything goes wrong
            if conn:
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

    logger.info("Running AI summary generation locally...")

    try:
        conn = get_db_connection()

        logger.info("Fetching data from RDS...")
        outages_data = fetch_power_outages(conn, hours=24)
        generation_data = fetch_power_generation(conn, hours=24)
        pricing_data = fetch_system_pricing(conn, hours=24)
        carbon_data = fetch_carbon_intensity(conn, hours=24)

        conn.close()
        logger.info("Database connection closed")

        all_data = {
            'outages': outages_data,
            'generation': generation_data,
            'pricing': pricing_data,
            'carbon': carbon_data
        }

        logger.info("Generating AI summary...")
        ai_summary = generate_openai_summary(all_data)

        logger.info("="*80)
        logger.info("GENERATED SUMMARY:")
        logger.info(ai_summary)
        logger.info("="*80)

        print("\n✅ Summary generated successfully!")
        print(f"\nData processed:")
        print(f"- Outages: {outages_data['total_outages']}")
        print(f"- Generation: {generation_data['total_generation_mw']} MW")
        print(f"- Avg Price: £{pricing_data['average_price']}/MWh")
        print(f"- Carbon: {carbon_data['average_intensity']} gCO2/kWh")

    except Exception as e:
        logger.error(f"Local test failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
