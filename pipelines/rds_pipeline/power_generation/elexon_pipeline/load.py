'''Load Elexon generation data to the RDS database.'''
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def get_db_connection():
    '''Establish a connection to the RDS database.'''
    try:
        connection = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='charliealston'
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

from psycopg2.extras import execute_values

def load_settlement_data_to_db(connection, settlement_df):
    '''
    Load settlement data and return settlement_ids mapped to each row.
    Returns IDs for both new and existing records.
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return None
    
    cursor = connection.cursor()
    
    # Get UNIQUE settlement combinations (preserve order)
    # Create tuples of (date, settlement_period)
    settlement_tuples = list(settlement_df[['date', 'settlement_period']].itertuples(index=False, name=None))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_settlements = []
    for settlement in settlement_tuples:
        if settlement not in seen:
            seen.add(settlement)
            unique_settlements.append(settlement)
    
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
    
    return settlement_ids


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
        None
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return
    
    cursor = connection.cursor()
    
    # Load settlement data and get settlement_ids
    settlement_ids = load_settlement_data_to_db(connection, price_df)
    
    # Prepare price data with settlement_ids
    data = [
        (
            settlement_ids[index],
            row['system_sell_price']
        )
        for index, row in price_df.iterrows()
    ]
    
    insert_query = '''
        INSERT INTO system_price (settlement_id, system_price)
        VALUES %s
        ON CONFLICT (settlement_id) DO NOTHING;
    '''
    
    execute_values(cursor, insert_query, data)
    connection.commit()
    
    print(f"Price data loaded successfully. {len(data)} records processed.")


from psycopg2.extras import execute_values

def load_fuel_types_to_db(connection, generation_df: pd.DataFrame):
    '''
    Load fuel types and return fuel_type_ids mapped to each row.
    Returns IDs for both new and existing fuel types.
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return None
    
    cursor = connection.cursor()
    
    # Get UNIQUE fuel types (preserve order of first appearance)
    unique_fuel_types = generation_df['fuel_type'].unique()
    fuel_type_tuples = [(ft,) for ft in unique_fuel_types]
    
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
    
    return fuel_type_ids



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
        None
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return
    
    cursor = connection.cursor()
    
    # Load settlements and get settlement_ids
    settlement_ids = load_settlement_data_to_db(connection, generation_df)
    
    # Load fuel types and get fuel_type_ids
    fuel_type_ids = load_fuel_types_to_db(connection, generation_df)
    
    # Prepare generation data with both foreign keys
    data = [
        (
            settlement_ids[index],
            fuel_type_ids[index],
            row['generation']
        )
        for index, row in generation_df.iterrows()
    ]
    
    insert_query = '''
        INSERT INTO generation (settlement_id, fuel_type_id, generation_mw)
        VALUES %s
        ON CONFLICT (settlement_id, fuel_type_id) DO NOTHING;
    '''
    
    execute_values(cursor, insert_query, data)
    connection.commit()
    
    print(f"Generation data loaded successfully. {len(data)} records processed.")

if __name__ == '__main__':
    from extract import fetch_elexon_generation_data, parse_elexon_price_data, fetch_elexon_price_data
    from transform import update_price_column_names,transform_generation_data
    import datetime
    start_time = datetime.datetime(2024, 1, 1)
    end_time = datetime.datetime(2025, 1, 2)
    raw_generation_data = fetch_elexon_generation_data(start_time, end_time)
    transformed_generation_data = transform_generation_data(raw_generation_data)
    print("Transformed Generation Data:")
    #print number of rows
    print(f"Number of rows: {len(transformed_generation_data)}")
    #print number of unique fuel types
    print(f"Number of unique fuel types: {transformed_generation_data['fuel_type'].nunique()}")
    print(transformed_generation_data.head())
    load_generation_data_to_db(get_db_connection(), transformed_generation_data)

