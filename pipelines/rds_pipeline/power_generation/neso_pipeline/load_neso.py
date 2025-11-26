'''Load script for NESO Power Generation data pipeline.'''
import logging
import os
import json
import boto3
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
#pylint: disable = logging-fstring-interpolation
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

    # Decrypts secret using the associated KMS key.
    secret = response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict


def load_secrets_to_env(secrets: dict):
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

def load_settlement_data_to_db(connection, settlement_df: pd.DataFrame) -> list:
    '''
    Load the settlement data into the RDS database and returns settlement_ids.
    Efficiently handles both new inserts and existing records.

    Args:
        connection: psycopg2 database connection object
        settlement_df (pd.DataFrame): DataFrame containing settlement data
    
    Returns:
        list: List of settlement_ids corresponding to the inserted/updated records
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return None

    try:
        logger.info(f"Loading {len(settlement_df)} settlement records")
        cursor = connection.cursor()

        # Prepare data as list of tuples
        data = [(row['settlement_date'], row['settlement_period']) for _, row in settlement_df.iterrows()]

        insert_query = '''
            INSERT INTO settlements (settlement_date, settlement_period)
            VALUES %s
            ON CONFLICT (settlement_date, settlement_period)
            DO UPDATE SET settlement_date = EXCLUDED.settlement_date
            RETURNING settlement_id;
        '''

        # execute_values is much faster than looping
        settlement_ids = execute_values(
            cursor,
            insert_query,
            data,
            fetch=True
        )

        connection.commit()
        logger.info(f"Successfully loaded {len(settlement_ids)} settlement records")

        # Extract IDs from result tuples
        return [row[0] for row in settlement_ids]

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading settlement data: {e}")
        return None
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing column: {e}")
        return None

def load_neso_demand_data_to_db(connection, demand_df: pd.DataFrame) -> bool:
    '''
    Load NESO demand data into the database.
    First load settlements to get settlement_ids, then load demand data.

    Args:
        connection: psycopg2 database connection
        demand_df (pd.DataFrame): Transformed NESO demand data with columns:
                                  'settlement_date', 'settlement_period',
                                  'national_demand', 'transmission_system_demand'
    Returns:
        bool: True if successful, False otherwise
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return False

    try:
        logger.info(f"Starting NESO demand data load for {len(demand_df)} records")
        cursor = connection.cursor()

        # Load settlements and get settlement_ids
        settlement_ids = load_settlement_data_to_db(connection, demand_df)

        if settlement_ids is None:
            logger.error("Failed to load settlement data. Aborting demand data load.")
            return False

        # Prepare demand data with settlement_ids
        data = [
            (
                settlement_ids[i],
                row['national_demand'],
                row['transmission_system_demand']
            )
            for i, (_, row) in enumerate(demand_df.iterrows())
        ]

        insert_query = '''
            INSERT INTO recent_demand (settlement_id, national_demand, transmission_system_demand)
            VALUES %s
            ON CONFLICT (settlement_id) 
            DO UPDATE SET 
                national_demand = EXCLUDED.national_demand,
                transmission_system_demand = EXCLUDED.transmission_system_demand;
        '''

        execute_values(cursor, insert_query, data)
        connection.commit()
        logger.info(f"Demand data loaded successfully. {len(data)} records processed.")
        return True

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading demand data: {e}")
        return False
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing expected column in demand data: {e}")
        return False

if __name__ == '__main__':
    from extract_neso import fetch_neso_demand_data, parse_neso_demand_data
    from transform_neso import transform_neso_demand_data
    secrets = get_secrets()
    load_secrets_to_env(secrets)
    conn = connect_to_database()
    raw_data = fetch_neso_demand_data("2025-11-01", 10)
    if raw_data is not None:
        parsed_data = parse_neso_demand_data(raw_data)
        transformed_data = transform_neso_demand_data(parsed_data)
        load_neso_demand_data_to_db(conn, transformed_data)
