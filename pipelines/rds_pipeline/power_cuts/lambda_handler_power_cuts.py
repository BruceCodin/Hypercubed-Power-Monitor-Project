"""AWS Lambda handler for power cuts data pipelines."""

import logging
import os
import json

import boto3
import psycopg2

from national_grid_pipeline.extract_national_grid import extract_data_national_grid
from national_grid_pipeline.transform_national_grid import transform_data_national_grid

from nie_networks_pipeline.extract_nie import extract_nie_data
from nie_networks_pipeline.transform_nie import transform_nie_data

from northern_powergrid_pipeline.extract_northern_powergrid import extract_northern_powergrid_data
from northern_powergrid_pipeline.transform_northern_powergrid import transform_northern_powergrid_data

from sp_energy_pipeline.extract_sp_en import extract_data_sp_en
from sp_energy_pipeline.transform_sp_en import transform_data_sp_en

from sp_northwest_pipeline.extract_sp_northwest import extract_data_sp_northwest
from sp_northwest_pipeline.transform_sp_northwest import transform_data_sp_northwest

from ssen_pipeline.extract_ssen import extract_ssen_data
from ssen_pipeline.transform_ssen import transform_ssen_data

from uk_power_networks_pipeline.extract_uk_pow import extract_data_uk_pow
from uk_power_networks_pipeline.transform_uk_pow import transform_data_uk_pow


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s - %(filename)s', level=logging.INFO)
logger = logging.getLogger()

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


def load_secrets_to_env(secrets: dict):
    """Load database credentials from Secrets Manager into environment variables.

    Args:
        secrets (dict): Dictionary containing database credentials"""

    for key, value in secrets.items():
        print(key, value)
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


def insert_data(conn, data):
    """Insert power cut data into the database.

    Args:
        conn: psycopg2 connection object
        data: List of dictionaries containing power cut data
    """

    cursor = conn.cursor()
    number_inserted = 0

    for entry in data:
        cursor.execute('''
            INSERT INTO FACT_outage (source_provider, outage_date, recording_time, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_provider, outage_date) DO NOTHING
            RETURNING outage_id
        ''', (
            entry['source_provider'],
            entry['outage_date'],
            entry['recording_time'],
            entry['status']
        ))

        result = cursor.fetchone()

        if result:
            new_outage_id = result[0]
            postcodes = entry['affected_postcodes']
            bridge_data = [(new_outage_id, code) for code in postcodes]

            cursor.executemany('''
                INSERT INTO BRIDGE_affected_postcodes (outage_id, postcode_affected)
                VALUES (%s, %s)
            ''', bridge_data)

            number_inserted += 1

    conn.commit()
    return number_inserted


def lambda_handler(event, context):
    """AWS Lambda handler function for power cuts ETL pipelines."""

    # Load secrets from AWS Secrets Manager and set as environment variables
    secrets = get_secrets()
    load_secrets_to_env(secrets)

    logging.info("Starting power cuts ETL execution")

    try:
        db_conn = connect_to_database()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

    # National Grid
    raw_data = extract_data_national_grid()
    if raw_data:
        transformed_data = transform_data_national_grid(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from National Grid")

    # NIE Networks
    raw_data = extract_nie_data()
    if raw_data:
        transformed_data = transform_nie_data(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from NIE Networks")

    # Northern Powergrid
    raw_data = extract_northern_powergrid_data()
    if raw_data:
        transformed_data = transform_northern_powergrid_data(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from Northern Powergrid")

    # SP Energy Networks
    raw_data = extract_data_sp_en()
    if raw_data:
        transformed_data = transform_data_sp_en(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from SP Energy Networks")

    # SP Northwest
    raw_data = extract_data_sp_northwest()
    if raw_data:
        transformed_data = transform_data_sp_northwest(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from SP Northwest")

    # SSEN
    raw_data = extract_ssen_data()
    if raw_data:
        transformed_data = transform_ssen_data(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from SSEN")

    # UK Power Networks
    raw_data = extract_data_uk_pow()
    if raw_data:
        transformed_data = transform_data_uk_pow(raw_data)
        insert_data(db_conn, transformed_data)
    else:
        logger.warning("No data from UK Power Networks")

    db_conn.close()

    return {
        'statusCode': 200,
        'body': 'Power cuts ETL execution completed'
    }


if __name__ == "__main__":

    # For local testing purposes

    response = lambda_handler(None, None)
    print(response)
