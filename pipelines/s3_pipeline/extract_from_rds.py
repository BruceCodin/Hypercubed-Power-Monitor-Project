import json
import os
import logging

import boto3
import psycopg2
import pandas as pd

SECRETS_ARN = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"

logger = logging.getLogger(__name__)


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


def get_historical_power_cut_data() -> pd.DataFrame:
    """Fetch historical power cut data from the database.

    Returns:
        pd.DataFrame: DataFrame containing historical power cut data
    """

    logger.info("Loading database secrets from AWS Secrets Manager...")
    secrets = get_secrets()
    logger.info("Secrets loaded successfully.")

    logger.info("Setting environment variables...")
    load_secrets_to_env(secrets)
    logger.info("Environment variables set.")

    logger.info("Connecting to the database...")
    conn = connect_to_database()
    logger.info("Database connection established.")

    cursor = conn.cursor()

    logger.info("Executing query to fetch historical power cut data...")
    query = """
    SELECT
        fo.outage_id,
        fo.source_provider,
        fo.status,
        fo.outage_date,
        fo.recording_time,
        bap.affected_id,
        bap.postcode_affected
    FROM fact_outage fo
    JOIN bridge_affected_postcodes bap 
    ON fo.outage_id = bap.outage_id;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    logger.info("Query executed successfully. Retrieved %d records.", len(rows))

    columns = ["outage_id", "source_provider", "status", "outage_date",
               "recording_time", "affected_id", "outage_id", "postcode_affected"]

    df = pd.DataFrame(rows, columns=columns)
    clean_df = df.T.drop_duplicates().T

    return clean_df


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    data = get_historical_power_cut_data()
    print(data.head(100))  # Print first 100 records
