'''Load carbon generation data to the RDS database.'''
import psycopg2
from psycopg2.extras import execute_values
def get_db_connection():
    '''Establish a connection to the RDS database.'''
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="charliealston"
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def load_settlement_data_to_db(connection, settlement_df):
    '''
    Load the settlement data into the RDS database and returns settlement_ids.
    Efficiently handles both new inserts and existing records.
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return None
    
    cursor = connection.cursor()
    
    # Prepare data as list of tuples
    data = [(row['date'], row['settlement_period']) for _, row in settlement_df.iterrows()]
    
    # Bulk insert with ON CONFLICT handling
    # The trick: DO UPDATE with a dummy update to get RETURNING to work for existing rows
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
    
    # Extract IDs from result tuples
    return [row[0] for row in settlement_ids]

def load_carbon_data_to_db(connection, carbon_df, settlement_ids):
    '''
    Load the carbon generation data into the RDS database.
    Uses settlement_ids from the settlements table.
    '''
    if connection is None:
        print("Failed to connect to the database. Data load aborted.")
        return
    
    cursor = connection.cursor()
    
    # Prepare data with settlement_ids matched to each row
    data = [
        (
            settlement_ids[index],
            row['intensity_forecast'],
            row['intensity_actual'],
            row['carbon_index']
        )
        for index, row in carbon_df.iterrows()
    ]
    
    insert_query = '''
        INSERT INTO carbon_intensity (settlement_id, intensity_forecast, intensity_actual, intensity_index)
        VALUES %s
        ON CONFLICT (settlement_id) DO NOTHING;
    '''
    
    execute_values(cursor, insert_query, data)
    connection.commit()
    
    print(f"Carbon data loaded successfully. {len(data)} records processed.")


if __name__ == "__main__":
    print("This module is intended to be imported and used within the ETL pipeline.")
    from extract import fetch_carbon_intensity_data
    from transform import transform_carbon_data
    from datetime import datetime
    raw_data = fetch_carbon_intensity_data(
        from_datetime = datetime(2025, 1, 1, 0, 0),
        to_datetime = datetime(2025, 1, 2, 0, 0)
    )
    connection = get_db_connection()
    transformed_data = transform_carbon_data(raw_data)
    settlement_ids = load_settlement_data_to_db(connection, transformed_data)
    load_carbon_data_to_db(connection, transformed_data, settlement_ids)
    connection.commit()
    connection.close()
