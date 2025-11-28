"""Functions for extracting alert data from RDS database."""

import logging
import json
import os

import psycopg2
import boto3

logger = logging.getLogger(__name__)

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

    secret = response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict


def load_secrets_to_env(secrets: dict) -> None:
    """Load database credentials from Secrets Manager into environment variables.

    Args:
        secrets (dict): Dictionary containing database credentials
    """

    for key, value in secrets.items():
        os.environ[key] = str(value)


def connect_to_database() -> psycopg2.extensions.connection:
    """Connects to AWS Postgres database using Secrets Manager credentials.

    Returns:
        psycopg2.extensions.connection: Database connection object
    """

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT")),
    )

    return conn


def get_alerts_to_send(conn: psycopg2.extensions.connection) -> list:
    """Query the database to find customers who need outage alerts.

    This function implements anti-spam logic by:
    1. Only selecting customers who haven't been notified for this specific outage
    2. Grouping postcodes together to send one email per customer per outage
    3. Using STRING_AGG to create comma-separated postcode lists

    Args:
        conn (psycopg2.extensions.connection): Database connection object

    Returns:
        list: List of tuples containing (customer_id, first_name, email,
              outage_id, outage_date, postcodes)
    """

    cursor = conn.cursor()

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
    LEFT JOIN FACT_notification_log AS log
        ON c.customer_id = log.customer_id
        AND o.outage_id = log.outage_id
    WHERE
        log.notification_id IS NULL
    GROUP BY
        c.customer_id, c.first_name, c.email, o.outage_id, o.outage_date;
    """

    cursor.execute(query)
    alerts = cursor.fetchall()

    cursor.close()

    logger.info("Found %d pending notifications from database", len(alerts))

    return alerts


if __name__ == "__main__":
    # For local testing purposes

    secrets = get_secrets()
    load_secrets_to_env(secrets)
    connection = connect_to_database()
    alerts = get_alerts_to_send(connection)

    if alerts:
        for alert in alerts:
            print(alert)
    else:
        print("No pending alerts found.")
