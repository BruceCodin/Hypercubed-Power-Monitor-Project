'''Load script for NESO Power Generation data pipeline.'''
import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
#pylint: disable = logging-fstring-interpolation
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.

    Returns:
        connection: psycopg2 connection object or None if connection fails.
    """
    try:
        logger.info("Attempting to connect to the database")
        # Will change to RDS credentials later
        connection = psycopg2.connect(
            dbname="postgres",
            user="charliealston",
            host="localhost",
        )
        logger.info("Successfully connected to the database")
        return connection
    except psycopg2.OperationalError as e:
        logger.error(f"Operational error connecting to database: {e}")
        return None

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

def load_neso_demand_data_to_db(connection, demand_df: pd.DataFrame, table: str) -> bool:
    '''
    Load NESO demand data into the database.
    First load settlements to get settlement_ids, then load demand data.

    Args:
        connection: psycopg2 database connection
        demand_df (pd.DataFrame): Transformed NESO demand data with columns:
                                  'settlement_date', 'settlement_period',
                                  'national_demand', 'transmission_system_demand'
        table (str): Target table name for demand data (historic_demand or recent_demand)

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

        insert_query = f'''
            INSERT INTO {table} (settlement_id, national_demand, transmission_system_demand)
            VALUES %s
            ON CONFLICT (settlement_id) DO NOTHING;
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
    from extract import fetch_neso_demand_data, parse_neso_demand_data
    from transform import transform_neso_demand_data
    conn = get_db_connection()
    #historical resource id for NESO demand data
    #historical test
    HISTORICAL_RESOURCE_ID = "b2bde559-3455-4021-b179-dfe60c0337b0"
    raw_data = fetch_neso_demand_data(HISTORICAL_RESOURCE_ID)
    parsed_data = parse_neso_demand_data(raw_data)
    transformed_data = transform_neso_demand_data(parsed_data)
    #print length of transformed data
    print(len(transformed_data))
    load_neso_demand_data_to_db(conn, transformed_data, 'historic_demand')
    #demand resource id "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"
    #recent test
    RECENT_RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"
    raw_recent_data = fetch_neso_demand_data(RECENT_RESOURCE_ID)
    parsed_recent_data = parse_neso_demand_data(raw_recent_data)
    transformed_recent_data = transform_neso_demand_data(parsed_recent_data)
    print(len(transformed_recent_data))
    load_neso_demand_data_to_db(conn, transformed_recent_data, 'recent_demand')
