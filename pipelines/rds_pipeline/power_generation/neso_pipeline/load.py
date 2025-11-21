'''Load script for NESO Power Generation data pipeline.'''
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values


def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.

    Args:
        db_config (dict): Database configuration parameters.
    Returns:
        connection: psycopg2 connection object.
    """
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="charliealston",
            host="localhost",
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

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
    settlement_tuples = list(settlement_df[['settlement_date', 'settlement_period']].itertuples(index=False, name=None))
    
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
        settlement_map[(row['settlement_date'], row['settlement_period'])]
        for _, row in settlement_df.iterrows()
    ]
    
    return settlement_ids

def load_neso_demand_data_to_db(connection, demand_df):
    '''
    Load NESO demand data into the database.
    First load settlements to get settlement_ids, then load demand data.

    Args:
        connection: psycopg2 database connection
        demand_df (pd.DataFrame): Transformed NESO demand data with columns:
                                  'settlement_date', 'settlement_period',
                                  'national_demand', 'transmission_system_demand'
    
    Returns:
        None
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return
    
    cursor = connection.cursor()
    
    # Load settlements and get settlement_ids
    settlement_ids = load_settlement_data_to_db(connection, demand_df)
    
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
        INSERT INTO historic_demand (settlement_id, national_demand, transmission_system_demand)
        VALUES %s
        ON CONFLICT (settlement_id) DO NOTHING;
    '''
    
    execute_values(cursor, insert_query, data)
    connection.commit()
    
    print(f"Demand data loaded successfully. {len(data)} records processed.")

if __name__ == '__main__':
    from extract import fetch_neso_demand_data, parse_neso_demand_data
    from transform import transform_neso_demand_data
    connection = get_db_connection()
    historical_resource_id = "b2bde559-3455-4021-b179-dfe60c0337b0"
    raw_data = fetch_neso_demand_data(historical_resource_id)
    parsed_data = parse_neso_demand_data(raw_data)
    transformed_data = transform_neso_demand_data(parsed_data)
    #print length of transformed data
    print(len(transformed_data))
    load_neso_demand_data_to_db(connection, transformed_data)