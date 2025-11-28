"""AWS Lambda function to send power outage alerts to subscribed customers.

This Lambda function:
1. Connects to RDS database
2. Queries for customers who need outage notifications
3. Sends personalized emails via AWS SES
4. Logs sent notifications to prevent duplicates
"""

import logging

from extract_alerts_from_rds import (
    get_secrets,
    load_secrets_to_env,
    connect_to_database,
    get_alerts_to_send
)
from process_alerts import process_alerts


# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """AWS Lambda function to send outage alerts to subscribed customers.

    1. Query database for customers needing alerts (anti-spam logic included)
    2. Send emails via AWS SES
    3. Log sent notifications to prevent duplicate alerts

    Args:
        event: Lambda event object (not used)
        context: Lambda context object (not used)

    Returns:
        dict: Response with statusCode, body, and statistics
    """

    conn = None

    try:
        logger.info("Loading secrets from Secrets Manager...")
        secrets = get_secrets()
        load_secrets_to_env(secrets)
        logger.info("Secrets loaded into environment variables.")

        logger.info("Establishing database connection...")
        conn = connect_to_database()
        logger.info("Database connection established.")

        logger.info("Querying for pending notifications...")
        alerts_to_send = get_alerts_to_send(conn)
        logger.info("Found %d pending notifications.", len(alerts_to_send))

        logger.info("Processing alerts...")
        stats = process_alerts(conn, alerts_to_send)
        logger.info("Alerts processing complete.")

        return {
            'statusCode': 200,
            'body': 'Notification cycle complete',
            'statistics': stats
        }

    except Exception as e:
        logger.error("Critical error in lambda_handler: %s", e)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    # For local testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("Running alerts lambda locally...")
    response = lambda_handler(None, None)
    print(f"\nResponse: {response}")
