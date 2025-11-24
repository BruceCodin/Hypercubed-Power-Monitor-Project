"""Module to extract historical power generation data from AWS RDS Postgres database."""

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


def get_historical_power_generation_data(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Fetch historical power generation data from the database.

    Args:
        conn (psycopg2.extensions.connection): Database connection object

    Returns:
        pd.DataFrame: DataFrame containing historical power generation data
    """

    cursor = conn.cursor()

    logger.info("Executing query to fetch historical power generation data...")
    query = """
    SELECT 
        g.generation_id,
        g.fuel_type_id,
        g.generation_mw,
        s.settlement_id,
        s.settlement_period, 
        s.settlement_date
    FROM 
        generation g
    JOIN
        settlements s ON g.settlement_id = s.settlement_id;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    logger.info("Query executed successfully. Retrieved %d records.", len(rows))

    cursor.close()

    columns = ["generation_id",
               "fuel_type_id",
               "generation_mw",
               "settlement_id",
               "settlement_period",
               "settlement_date"]

    df = pd.DataFrame(rows, columns=columns)

    return df


def get_historical_carbon_data(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Fetch historical carbon intensity data from the database.

    Args:
        conn (psycopg2.extensions.connection): Database connection object

    Returns:
        pd.DataFrame: DataFrame containing historical carbon intensity data
    """

    cursor = conn.cursor()

    logger.info("Executing query to fetch historical carbon intensity data...")
    query = """
    SELECT 
        c.intensity_id,
        c.intensity_forecast,
        c.intensity_actual,
        c.intensity_index,
        c.settlement_id,
        s.settlement_date,
        s.settlement_period
    FROM 
        carbon_intensity c
    JOIN 
        settlements s 
    ON c.settlement_id = s.settlement_id;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    logger.info("Query executed successfully. Retrieved %d records.", len(rows))

    cursor.close()

    columns = ["intensity_id",
               "intensity_forecast",
               "intensity_actual",
               "intensity_index",
               "settlement_id",
               "settlement_date",
               "settlement_period"]

    df = pd.DataFrame(rows, columns=columns)

    return df


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    secrets = get_secrets()
    load_secrets_to_env(secrets)
    conn = connect_to_database()

    generation_data = get_historical_power_generation_data(conn)
    print(generation_data.head(10))  # Print first 10 records

    carbon_data = get_historical_carbon_data(conn)
    print(carbon_data.head(10))  # Print first 10 records
