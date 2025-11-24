import logging
import json
import os

import psycopg2
import boto3

ses_client = boto3.client('ses', region_name='eu-west-2')
secrets = boto3.client('secretsmanager')

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRETS_ARN = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager.

    Returns:
        dict: Dictionary containing database credentials
    """

    client = boto3.client('secretsmanager')

    response = client.get_secret_value(
        SecretId=SECRETS_ARN
    )

    # Decrypts secret using the associated KMS key.
    secret = response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict


def load_secrets_to_env(secrets: dict) -> None:
    """Load database credentials from Secrets Manager into environment variables.

    Args:
        secrets (dict): Dictionary containing database credentials"""

    for key, value in secrets.items():
        os.environ[key] = str(value)


def connect_to_database() -> psycopg2.extensions.connection:
    """Connects to AWS Postgres database using Secrets Manager credentials.

    Returns:
        psycopg2 connection object
    """

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT")),
    )

    return conn


def lambda_handler(event, context):
    """AWS Lambda function to send outage alerts to subscribed customers.
    1. Query database for customers needing alerts (anti-spam logic included).
    2. Send emails via AWS SES.
    3. Log sent notifications to prevent duplicate alerts.
    """

    logger.info("Loading secrets from Secrets Manager...")
    secrets = get_secrets()
    load_secrets_to_env(secrets)

    logger.info("Establishing database connection...")
    conn = connect_to_database()
    logger.info("Database connection established.")

    cursor = conn.cursor()

    try:
        # --- STEP 1: FIND WHO NEEDS EMAILING ---
        # This is the "Anti-Spam" logic.
        # UPDATED: We now GROUP BY customer and outage to prevent duplicates.
        # We use STRING_AGG to list all affected postcodes in one single email.

        query = """
        SELECT 
            c.customer_id,
            c.first_name, 
            c.email, 
            o.outage_id, 
            o.outage_date,
            STRING_AGG(DISTINCT bsp.postcode, ', ') as postcodes
        FROM FACT_outage o
        JOIN BRIDGE_affected_postcodes AS bap 
            ON o.outage_id = bap.outage_id
        JOIN BRIDGE_subscribed_postcodes AS bsp 
            ON bap.postcode_affected = bsp.postcode
        JOIN DIM_customer AS c 
            ON bsp.customer_id = c.customer_id
        -- The Anti-Spam Join:
        LEFT JOIN FACT_notification_log AS log 
            ON c.customer_id = log.customer_id 
            AND o.outage_id = log.outage_id
        WHERE 
            log.notification_id IS NULL -- Only if we haven't emailed them yet
        GROUP BY 
            c.customer_id, c.first_name, c.email, o.outage_id, o.outage_date;
        """

        cursor.execute(query)
        alerts_to_send = cursor.fetchall()

        logger.info("Found %d pending notifications.", len(alerts_to_send))

        # --- STEP 2: SEND EMAILS AND LOG THEM ---

        for row in alerts_to_send:
            # row now contains the aggregated string of postcodes (e.g. "SW1, SW2")
            customer_id, first_name, email, outage_id, outage_time, postcode_list = row

            # A. Send the Email via AWS SES
            subject = f"Power Outage Alert for {postcode_list}"
            body = f"Hi {first_name},\n\nThere is a power outage affecting the following postcodes: {postcode_list}.\nReported at {outage_time}. We are working on it."

            try:
                response = ses_client.send_email(
                    Source='mohammadmuarijb@yahoo.co.uk',
                    Destination={'ToAddresses': [email]},
                    Message={
                        'Subject': {'Data': subject},
                        'Body': {'Text': {'Data': body}}
                    }
                )
                logger.info("Email sent to %s for Outage %d", email, outage_id)

                # B. Update the Log Table (The "Mark as Read" step)
                # This ensures they won't be picked up by the query next time (5 mins later)
                log_insert = """
                INSERT INTO FACT_notification_log (customer_id, outage_id)
                VALUES (%s, %s)
                """
                cursor.execute(log_insert, (customer_id, outage_id))

                # Commit after every successful email to be safe,
                conn.commit()

            except Exception as e:
                logger.error("Failed to send/log for %s: %s", email, e)
                conn.rollback()  # Rollback this specific transaction if it fails

    except Exception as e:
        logger.error("Critical Database Error: %s", e)
    finally:
        cursor.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': 'Notification cycle complete'
    }


if __name__ == "__main__":
    # For local testing
    print("Running alerts lambda locally...")
    response = lambda_handler(None, None)
    print(f"\nResponse: {response}")
