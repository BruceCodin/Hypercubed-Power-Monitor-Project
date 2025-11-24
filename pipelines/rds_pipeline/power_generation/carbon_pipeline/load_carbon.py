'''Load carbon generation data to the RDS database.'''
import logging
import os
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values
#pylint: disable=logging-fstring-interpolation


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
def load_settlement_data_to_db(connection, settlement_df):
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
        data = [(row['date'], row['settlement_period']) for _, row in settlement_df.iterrows()]

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

def load_carbon_data_to_db(connection, carbon_df):
    '''
    Load the carbon generation data into the RDS database.
    Uses settlement_ids from the settlements table.
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return False

    try:
        logger.info(f"Starting carbon data load for {len(carbon_df)} records")
        cursor = connection.cursor()
        settlement_ids = load_settlement_data_to_db(connection, carbon_df)

        if settlement_ids is None:
            logger.error("Failed to load settlement data. Aborting carbon data load.")
            return False

        # Prepare data with settlement_ids matched to each row
        data = [
        (
            settlement_ids[i],
            row['intensity_forecast'],
            row['intensity_actual'],
            row['carbon_index']
        )
        for i, (_, row) in enumerate(carbon_df.iterrows())
    ]

        insert_query = '''
                    INSERT INTO carbon_intensity (settlement_id, intensity_forecast, intensity_actual, intensity_index)
                    VALUES %s
                    ON CONFLICT (settlement_id) 
                    DO UPDATE SET 
                        intensity_forecast = EXCLUDED.intensity_forecast,
                        intensity_actual = EXCLUDED.intensity_actual,
                        intensity_index = EXCLUDED.intensity_index;
                        '''

        execute_values(cursor, insert_query, data)
        connection.commit()

        logger.info(f"Carbon data loaded successfully. {len(data)} records processed.")
        return True
    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading carbon data: {e}")
        return False
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing expected column in carbon data: {e}")
        return False


if __name__ == "__main__":
    print("This module is intended to be imported and used within the ETL pipeline.")
    from extract_carbon import fetch_carbon_intensity_data
    from transform_carbon import transform_carbon_data
    from datetime import datetime
    # carbon intensity api can only fetch data in month chunks
    raw_data = fetch_carbon_intensity_data(
        from_datetime = datetime(2025, 3, 1, 0, 0),
        to_datetime = datetime(2025, 3, 28, 0, 0)
    )
    tranform_data = transform_carbon_data(raw_data)
    print(tranform_data.head())
    secrets= get_secrets()
    load_secrets_to_env(secrets)
    db_connection = connect_to_database()
    # uploads carbon data to rds
    load_carbon_data_to_db(db_connection, tranform_data)
