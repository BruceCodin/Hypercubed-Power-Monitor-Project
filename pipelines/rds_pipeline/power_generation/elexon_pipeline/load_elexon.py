'''Load Elexon generation data to the RDS database.'''
import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

logger = logging.getLogger(__name__)

def get_db_connection():
    '''Establish a connection to the RDS database.'''
    try:
        logger.info("Attempting to connect to the database")
        # Will change to RDS credentials later
        connection = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='charliealston'
        )
        logger.info("Successfully connected to the database")
        return connection
    except psycopg2.OperationalError as e:
        logger.error(f"Operational error connecting to database: {e}")
        return None

def load_settlement_data_to_db(connection, settlement_df):
    '''
    Load settlement data and return settlement_ids mapped to each row.
    Returns IDs for both new and existing records.
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return None

    try:
        logger.info(f"Loading settlement data for {len(settlement_df)} records")
        cursor = connection.cursor()

        settlement_tuples = list(settlement_df[['date', 'settlement_period']].itertuples(index=False, name=None))

        # Remove duplicates while preserving order
        seen = set()
        unique_settlements = []
        for settlement in settlement_tuples:
            if settlement not in seen:
                seen.add(settlement)
                unique_settlements.append(settlement)

        logger.info(f"Inserting {len(unique_settlements)} unique settlements")

        insert_query = '''
            INSERT INTO settlements (settlement_date, settlement_period)
            VALUES %s
            ON CONFLICT (settlement_date, settlement_period)
            DO UPDATE SET settlement_date = EXCLUDED.settlement_date
            RETURNING settlement_id, settlement_date, settlement_period;
        '''

        results = execute_values(cursor, insert_query, unique_settlements, fetch=True)
        connection.commit()

        # Create mapping: {(date, period): settlement_id}
        settlement_map = {(row[1], row[2]): row[0] for row in results}

        # Map back to original DataFrame order
        settlement_ids = [
            settlement_map[(row['date'], row['settlement_period'])]
            for _, row in settlement_df.iterrows()
        ]

        logger.info(f"Successfully loaded {len(results)} settlement records")
        return settlement_ids

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading settlement data: {e}")
        return None
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing column in settlement data: {e}")
        return None

def load_price_data_to_db(connection, price_df: pd.DataFrame):
    '''
    Load price data into RDS database.
    First loads settlement data to get settlement_ids,
    then loads price data linked to those settlement_ids.

    Args:
        connection: psycopg2 connection object
        price_df: DataFrame with price data including 'date', 'settlement_period',
                  and 'system_sell_price' columns

    Returns:
        bool: True if successful, False otherwise
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return False

    try:
        logger.info(f"Starting price data load for {len(price_df)} records")
        cursor = connection.cursor()

        # Load settlement data and get settlement_ids
        settlement_ids = load_settlement_data_to_db(connection, price_df)

        if settlement_ids is None:
            logger.error("Failed to load settlement data. Aborting price data load.")
            return False

        # Prepare price data with settlement_ids
        data = [
        (
            settlement_ids[i],
            row['system_sell_price']
        )
        for i, (_, row) in enumerate(price_df.iterrows())
    ]

        insert_query = '''
            INSERT INTO system_price (settlement_id, system_price)
            VALUES %s
            ON CONFLICT (settlement_id) DO NOTHING;
        '''

        execute_values(cursor, insert_query, data)
        connection.commit()

        logger.info(f"Price data loaded successfully. {len(data)} records processed.")
        return True

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading price data: {e}")
        return False
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing expected column in price data: {e}")
        return False

def load_fuel_types_to_db(connection, generation_df: pd.DataFrame):
    '''
    Load fuel types and return fuel_type_ids mapped to each row.
    Returns IDs for both new and existing fuel types.
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return None

    try:
        logger.info(f"Loading fuel types from {len(generation_df)} records")
        cursor = connection.cursor()

        # Get UNIQUE fuel types (preserve order of first appearance)
        unique_fuel_types = generation_df['fuel_type'].unique()
        fuel_type_tuples = [(ft,) for ft in unique_fuel_types]

        logger.info(f"Inserting {len(unique_fuel_types)} unique fuel types")

        insert_query = '''
            INSERT INTO fuel_type (fuel_type)
            VALUES %s
            ON CONFLICT (fuel_type)
            DO UPDATE SET fuel_type = EXCLUDED.fuel_type
            RETURNING fuel_type_id, fuel_type;
        '''

        results = execute_values(cursor, insert_query, fuel_type_tuples, fetch=True)
        connection.commit()

        # Create mapping: {fuel_type_name: fuel_type_id}
        fuel_type_map = {row[1]: row[0] for row in results}

        # Map back to original DataFrame order
        fuel_type_ids = [fuel_type_map[ft] for ft in generation_df['fuel_type']]

        logger.info(f"Successfully loaded {len(results)} fuel types")
        return fuel_type_ids

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading fuel types: {e}")
        return None
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing 'fuel_type' column in data: {e}")
        return None

def load_generation_data_to_db(connection, generation_df: pd.DataFrame):
    '''
    Load generation data into RDS database.
    First loads settlement data to get settlement_ids,
    Then loads fuel types to get fuel_type_ids,
    then loads generation data linked to those settlement_ids.

    Args:
        connection: psycopg2 connection object
        generation_df: DataFrame with generation data including 'settlement_date',
                       'settlement_period', 'fuel_type', and 'generation' columns.

    Returns:
        bool: True if successful, False otherwise
    '''
    if connection is None:
        logger.error("No database connection provided. Data load aborted.")
        return False

    try:
        logger.info(f"Starting generation data load for {len(generation_df)} records")
        cursor = connection.cursor()

        # Load settlements and get settlement_ids
        settlement_ids = load_settlement_data_to_db(connection, generation_df)

        if settlement_ids is None:
            logger.error("Failed to load settlement data. Aborting generation data load.")
            return False

        # Load fuel types and get fuel_type_ids
        fuel_type_ids = load_fuel_types_to_db(connection, generation_df)

        if fuel_type_ids is None:
            logger.error("Failed to load fuel types. Aborting generation data load.")
            return False

        # Prepare generation data with both foreign keys
        data = [
        (
            settlement_ids[i],
            fuel_type_ids[i],   
            row['generation']
        )
        for i, (_, row) in enumerate(generation_df.iterrows())
    ]

        insert_query = '''
            INSERT INTO generation (settlement_id, fuel_type_id, generation_mw)
            VALUES %s
            ON CONFLICT (settlement_id, fuel_type_id) DO NOTHING;
        '''

        execute_values(cursor, insert_query, data)
        connection.commit()

        logger.info(f"Generation data loaded successfully. {len(data)} records processed.")
        return True

    except psycopg2.IntegrityError as e:
        connection.rollback()
        logger.error(f"Integrity error while loading generation data: {e}")
        return False
    except KeyError as e:
        connection.rollback()
        logger.error(f"Missing expected column in generation data: {e}")
        return False


if __name__ == '__main__':
    from pipelines.rds_pipeline.power_generation.elexon_pipeline.extract_elexon import fetch_elexon_generation_data, parse_elexon_price_data, fetch_elexon_price_data
    from pipelines.rds_pipeline.power_generation.elexon_pipeline.transform_elexon import update_price_column_names,transform_generation_data
    import datetime
    start_time = datetime.datetime(2025, 1, 2)
    end_time = datetime.datetime(2025, 1, 3)
    #generation data load test
    raw_generation_data = fetch_elexon_generation_data(start_time, end_time)
    transformed_generation_data = transform_generation_data(raw_generation_data)
    print(transformed_generation_data.head())
    load_generation_data_to_db(get_db_connection(), transformed_generation_data)
    #price data load test
    raw_price_data = fetch_elexon_price_data(start_time)
    parsed_price_data = parse_elexon_price_data(raw_price_data)
    transformed_price_data = update_price_column_names(parsed_price_data)
    print(transformed_price_data.head())
    load_price_data_to_db(get_db_connection(), transformed_price_data)

