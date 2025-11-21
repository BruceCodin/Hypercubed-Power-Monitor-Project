"""Load script for power cuts data pipelines.

This module orchestrates the extraction, transformation, and loading of power
cut data from multiple UK utility providers into the database."""

import logging
import os

import psycopg2
from dotenv import load_dotenv

###
#
# Import extraction and transformation functions from each pipeline
#
###

# National Grid Pipeline
from national_grid_pipeline.extract_national_grid import extract_data_national_grid
from national_grid_pipeline.transform_national_grid import transform_data_national_grid

# NIE Networks Pipeline
from nie_networks_pipeline.extract_nie import extract_NIE_data
from nie_networks_pipeline.transform_nie import transform_nie_data

# Northern Powergrid Pipeline
from northern_powergrid_pipeline.extract_northern_powergrid import extract_northern_powergrid_data
from northern_powergrid_pipeline.transform_northern_powergrid import transform_northern_powergrid_data

# SP Energy Networks Pipeline
from sp_energy_pipeline.extract_sp_en import extract_data_sp_en
from sp_energy_pipeline.transform_sp_en import transform_data_sp_en

# SP Northwest Pipeline
from sp_northwest_pipeline.extract_sp_northwest import extract_data_sp_northwest
from sp_northwest_pipeline.transform_sp_northwest import transform_data_sp_northwest

# SSEN Pipeline
from ssen_pipeline.extract_ssen import extract_power_cut_data as extract_ssen_raw
from ssen_pipeline.extract_ssen import parse_power_cut_data as parse_ssen_data
# Note: SSEN transform script is currently empty/placeholder
# from ssen_pipeline.transform_ssen import transform_ssen_data

# UK Power Networks Pipeline
from uk_power_networks_pipeline.extract_uk_pow import extract_data_uk_pow
from uk_power_networks_pipeline.transform_uk_pow import transform_data_uk_pow

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
    """Insert power cut data into the database.

    Args:
        conn: Database connection object
        data: List of power cut records to insert
    """
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

    # Connect to database and load data
    db_conn = connect_to_database()
    logging.info("Connected to database")

    # ========================================================================
    # Load: National Grid Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("NATIONAL GRID PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting National Grid power cuts data...")
        raw_data_national_grid = extract_data_national_grid()

        if raw_data_national_grid:
            logging.info(
                f"Extracted {len(raw_data_national_grid)} raw records from National Grid")

            logging.info("Transforming National Grid data...")
            transformed_data_national_grid = transform_data_national_grid(
                raw_data_national_grid)

            logging.info(
                f"Transformed {len(transformed_data_national_grid)} records from National Grid")

            logging.info("Inserting National Grid data into database...")
            insert_data(db_conn, transformed_data_national_grid)
            logging.info("National Grid data insertion complete.")
        else:
            logging.warning("No data extracted from National Grid. Skipping.")
    except Exception as e:
        logging.error(f"Error processing National Grid pipeline: {e}")

    # ========================================================================
    # Load: NIE Networks Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("NIE NETWORKS PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting NIE Networks power cuts data...")
        raw_data_nie = extract_NIE_data()

        if raw_data_nie:
            logging.info(
                f"Extracted {len(raw_data_nie)} raw records from NIE Networks")

            logging.info("Transforming NIE Networks data...")
            transformed_data_nie = transform_nie_data(raw_data_nie)

            logging.info(
                f"Transformed {len(transformed_data_nie)} records from NIE Networks")

            logging.info("Inserting NIE Networks data into database...")
            insert_data(db_conn, transformed_data_nie)
            logging.info("NIE Networks data insertion complete.")
        else:
            logging.warning("No data extracted from NIE Networks. Skipping.")
    except Exception as e:
        logging.error(f"Error processing NIE Networks pipeline: {e}")

    # ========================================================================
    # Load: Northern Powergrid Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("NORTHERN POWERGRID PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting Northern Powergrid power cuts data...")
        raw_data_northern = extract_northern_powergrid_data()

        if raw_data_northern:
            logging.info(
                f"Extracted {len(raw_data_northern)} raw records from Northern Powergrid")

            logging.info("Transforming Northern Powergrid data...")
            transformed_data_northern = transform_northern_powergrid_data(
                raw_data_northern)

            logging.info(
                f"Transformed {len(transformed_data_northern)} records from Northern Powergrid")

            logging.info("Inserting Northern Powergrid data into database...")
            insert_data(db_conn, transformed_data_northern)
            logging.info("Northern Powergrid data insertion complete.")
        else:
            logging.warning(
                "No data extracted from Northern Powergrid. Skipping.")
    except Exception as e:
        logging.error(f"Error processing Northern Powergrid pipeline: {e}")

    # ========================================================================
    # Load: SP Energy Networks Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("SP ENERGY NETWORKS PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting SP Energy Networks power cuts data...")
        raw_data_sp_en = extract_data_sp_en()

        if raw_data_sp_en:
            logging.info(
                f"Extracted {len(raw_data_sp_en)} raw records from SP Energy Networks")

            logging.info("Transforming SP Energy Networks data...")
            transformed_data_sp_en = transform_data_sp_en(raw_data_sp_en)

            logging.info(
                f"Transformed {len(transformed_data_sp_en)} records from SP Energy Networks")

            logging.info("Inserting SP Energy Networks data into database...")
            insert_data(db_conn, transformed_data_sp_en)
            logging.info("SP Energy Networks data insertion complete.")
        else:
            logging.warning(
                "No data extracted from SP Energy Networks. Skipping.")
    except Exception as e:
        logging.error(f"Error processing SP Energy Networks pipeline: {e}")

    # ========================================================================
    # Load: SP Northwest Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("SP NORTHWEST PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting SP Northwest power cuts data...")
        raw_data_sp_nw = extract_data_sp_northwest()

        if raw_data_sp_nw:
            logging.info(
                f"Extracted {len(raw_data_sp_nw)} raw records from SP Northwest")

            logging.info("Transforming SP Northwest data...")
            transformed_data_sp_nw = transform_data_sp_northwest(raw_data_sp_nw)

            logging.info(
                f"Transformed {len(transformed_data_sp_nw)} records from SP Northwest")

            logging.info("Inserting SP Northwest data into database...")
            insert_data(db_conn, transformed_data_sp_nw)
            logging.info("SP Northwest data insertion complete.")
        else:
            logging.warning("No data extracted from SP Northwest. Skipping.")
    except Exception as e:
        logging.error(f"Error processing SP Northwest pipeline: {e}")

    # ========================================================================
    # Load: SSEN Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("SSEN PIPELINE (PLACEHOLDER - Transform not implemented)")
    logging.info("=" * 80)

    try:
        logging.info("Extracting SSEN power cuts data...")
        raw_data_ssen = extract_ssen_raw()

        if raw_data_ssen:
            parsed_data_ssen = parse_ssen_data(raw_data_ssen)
            logging.info(
                f"Extracted and parsed {len(parsed_data_ssen)} records from SSEN")

            # TODO: Implement transform_ssen_data() function
            logging.warning(
                "SSEN transform not implemented. Skipping transformation and insertion.")

            # Placeholder for when transform is implemented:
            # logging.info("Transforming SSEN data...")
            # transformed_data_ssen = transform_ssen_data(parsed_data_ssen)
            # logging.info(f"Transformed {len(transformed_data_ssen)} records from SSEN")
            # logging.info("Inserting SSEN data into database...")
            # insert_data(db_conn, transformed_data_ssen)
            # logging.info("SSEN data insertion complete.")
        else:
            logging.warning("No data extracted from SSEN. Skipping.")
    except Exception as e:
        logging.error(f"Error processing SSEN pipeline: {e}")

    # ========================================================================
    # Load: UK Power Networks Data Pipeline
    # ========================================================================
    logging.info("=" * 80)
    logging.info("UK POWER NETWORKS PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Extracting UK Power Networks power cuts data...")
        raw_data_uk_pow = extract_data_uk_pow()

        if raw_data_uk_pow:
            logging.info(
                f"Extracted {len(raw_data_uk_pow)} raw records from UK Power Networks")

            logging.info("Transforming UK Power Networks data...")
            transformed_data_uk_pow = transform_data_uk_pow(raw_data_uk_pow)

            logging.info(
                f"Transformed {len(transformed_data_uk_pow)} records from UK Power Networks")

            logging.info("Inserting UK Power Networks data into database...")
            insert_data(db_conn, transformed_data_uk_pow)
            logging.info("UK Power Networks data insertion complete.")
        else:
            logging.warning(
                "No data extracted from UK Power Networks. Skipping.")
    except Exception as e:
        logging.error(f"Error processing UK Power Networks pipeline: {e}")

    # ========================================================================
    # Complete
    # ========================================================================
    logging.info("=" * 80)
    logging.info("ALL PIPELINES COMPLETE")
    logging.info("=" * 80)

    # Connection Closed
    db_conn.close()
    logging.info("Database connection closed")
