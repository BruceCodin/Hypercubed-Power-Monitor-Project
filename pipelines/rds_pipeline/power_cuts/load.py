import psycopg2
import logging
import os
from dotenv import load_dotenv

###
#
# Import extraction and transformation functions from each pipeline
#
###

# National Grid Pipeline
from national_grid_pipeline.extract_national_grid import extract_data_national_grid
from national_grid_pipeline.transform_national_grid import transform_data_national_grid

# Northern Ireland Electricity Networks Pipeline  
from nie_networks_pipeline.transform_nie import transform_nie_data

# Load environment variables
load_dotenv()


def connect_to_database():
    """Connects to your AWS Postgres database and ensures tables exist."""

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

    return conn


def insert_data(conn, data):

    cursor = conn.cursor()
    number_inserted = 0

    try:
        for entry in data:
            # 1. Insert into Parent Table (FACT_outage)
            # Using ON CONFLICT to handle duplicates based on the unique index
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

            # 2. Check if we actually inserted anything
            result = cursor.fetchone()

            if result:
                new_outage_id = result[0]

                # 3. Insert into the Child/Bridge Table (BRIDGE_affected_postcodes)
                postcodes = entry['affected_postcodes']

                bridge_data = [(new_outage_id, code) for code in postcodes]

                cursor.executemany('''
                    INSERT INTO BRIDGE_affected_postcodes (outage_id, postcode_affected)
                    VALUES (%s, %s)
                ''', bridge_data)

                number_inserted += 1

            else:
                pass

        conn.commit()
        logging.info(
            "Inserted %d records into FACT_outage and BRIDGE_affected_postcodes.", number_inserted)

        logging.info("Skipped %d duplicate records.",
                     len(data) - number_inserted)

    except (psycopg2.Error, Exception) as e:
        logging.error(f"An error occurred: {e}")
        conn.rollback()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # raw_data = extract_power_cut_data()
    # transformed_data = transform_power_cut_data(raw_data)

    # db_conn = connect_to_database()

    # insert_data(db_conn, transformed_data)

    # db_conn.close()

    # Connect to database and load data
    db_conn = connect_to_database()
    logging.info("Connected to database")

    # Load: National Grid Data Pipeline
    logging.info("Extracting National Grid power cuts data...")
    raw_data_national_grid = extract_data_national_grid()

    logging.info(f"Extracted {len(raw_data_national_grid)} raw records from National Grid")

    logging.info("Transforming National Grid data...")
    transformed_data_national_grid = transform_data_national_grid(raw_data_national_grid)

    logging.info(
        f"Transformed {len(transformed_data_national_grid)} records from National Grid")

    logging.info("Inserting National Grid data into database...")
    insert_data(db_conn, transformed_data_national_grid)
    logging.info("National Grid data insertion complete.")

    # Load: NIE Networks Data Pipeline
    logging.info("Transforming NIE Networks power cuts data...")
    transformed_data_nie = transform_nie_data()
    
    logging.info(f"Extracted and Transformed {len(transformed_data_nie)} records from NIE Networks")

    logging.info("Inserting NIE Networks data into database...")
    insert_data(db_conn, transformed_data_nie)
    logging.info("NIE Networks data insertion complete.")


    #Load: Northern Power Networks Data Pipeline
    # (To be implemented)


    # Connection Closed
    db_conn.close()
    logging.info("Database connection closed")

