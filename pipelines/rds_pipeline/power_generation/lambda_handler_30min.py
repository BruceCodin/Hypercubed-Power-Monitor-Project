"""
Consolidated 30-minute ETL pipeline handler.
Runs Carbon Intensity and Elexon (Generation + Price) data pipelines.
"""
import logging
import sys
import os
from datetime import datetime, timedelta

# Add pipeline directories to Python path (for local testing)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'carbon_pipeline'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'elexon_pipeline'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ============================================================
# CARBON INTENSITY FUNCTIONS
# ============================================================

def get_last_carbon_datetime(connection):
    """Get the most recent settlement datetime that has carbon data."""
    try:
        cursor = connection.cursor()
        query = """
            SELECT s.settlement_date, s.settlement_period
            FROM carbon_intensity ci
            JOIN settlements s ON ci.settlement_id = s.settlement_id
            ORDER BY s.settlement_date DESC, s.settlement_period DESC
            LIMIT 1;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            settlement_date, settlement_period = result
            logger.info("Last carbon data: %s period %s", settlement_date, settlement_period)
            return settlement_date, settlement_period

        logger.info("No existing carbon data found - first run")
        return None, None

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error getting last carbon datetime: %s", e)
        return None, None


def process_carbon_data(db_connection):
    """
    Run Carbon Intensity pipeline.

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        logger.info("=" * 60)
        logger.info("CARBON INTENSITY PIPELINE")
        logger.info("=" * 60)

        # pylint: disable=import-outside-toplevel
        from carbon_pipeline.extract_carbon import fetch_carbon_intensity_data
        from carbon_pipeline.transform_carbon import transform_carbon_data
        from carbon_pipeline.load_carbon import load_carbon_data_to_db

        # Get last datetime and calculate fetch window
        last_date, last_period = get_last_carbon_datetime(db_connection)
        end_time = datetime.now() + timedelta(minutes=5)

        if last_date is None:
            start_time = end_time - timedelta(days=7)
            logger.info("First run - fetching last 7 days: %s to %s", start_time, end_time)
        else:
            start_time = datetime.combine(last_date, datetime.min.time())
            start_time += timedelta(minutes=(last_period - 1) * 30)
            logger.info("Fetching from: %s (period %s) to %s", start_time, last_period, end_time)

        # Extract
        logger.info("Extracting carbon data from %s to %s", start_time, end_time)
        raw_carbon = fetch_carbon_intensity_data(start_time, end_time)

        if raw_carbon is None or len(raw_carbon) == 0:
            logger.warning("No carbon data from API")
            return True, "No new carbon data available"

        logger.info("Received %d records", len(raw_carbon))

        # Transform and load
        transformed_carbon = transform_carbon_data(raw_carbon)
        success = load_carbon_data_to_db(db_connection, transformed_carbon)

        if success:
            msg = f"Processed {len(transformed_carbon)} carbon records"
            logger.info("✓ %s", msg)
            return True, msg
        else:
            logger.error("✗ Failed to load carbon data")
            return False, "Failed to load carbon data"

    except ImportError as e:
        logger.error("Import error: %s", e, exc_info=True)
        return False, f"Import error: {str(e)}"
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Carbon pipeline error: %s", e, exc_info=True)
        return False, f"Pipeline error: {str(e)}"


# ============================================================
# ELEXON FUNCTIONS
# ============================================================

def get_last_elexon_datetime(connection, data_type):
    """Get the most recent settlement datetime for generation or price data."""
    try:
        cursor = connection.cursor()

        if data_type == "generation":
            table = "generation"
        else:  # price
            table = "system_price"

        query = f"""
            SELECT s.settlement_date, s.settlement_period
            FROM {table} sp
            JOIN settlements s ON sp.settlement_id = s.settlement_id
            ORDER BY s.settlement_date DESC, s.settlement_period DESC
            LIMIT 1;
        """

        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            settlement_date, settlement_period = result
            logger.info("Last %s data: %s period %s", data_type, settlement_date, settlement_period)
            return settlement_date, settlement_period

        logger.info("No existing %s data found - first run", data_type)
        return None, None

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error getting last %s datetime: %s", data_type, e)
        return None, None


def process_elexon_data(db_connection):
    """
    Run Elexon (Generation + Price) pipeline.

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        logger.info("=" * 60)
        logger.info("ELEXON PIPELINE")
        logger.info("=" * 60)

        # pylint: disable=import-outside-toplevel
        from elexon_pipeline.extract_elexon import (
            fetch_elexon_generation_data,
            fetch_elexon_price_data,
            parse_elexon_price_data
        )
        from elexon_pipeline.transform_elexon import (
            transform_generation_data,
            update_price_column_names
        )
        from elexon_pipeline.load_elexon import (
            load_generation_data_to_db,
            load_price_data_to_db
        )

        generation_success = False
        price_success = False

        # ---- Generation Data ----
        try:
            logger.info("[1/2] Processing Generation Data")
            last_gen_date, last_gen_period = get_last_elexon_datetime(db_connection, "generation")
            end_time = datetime.now() + timedelta(minutes=5)

            if last_gen_date is None:
                start_time = end_time - timedelta(days=7)
            else:
                start_time = datetime.combine(last_gen_date, datetime.min.time())
                start_time += timedelta(minutes=(last_gen_period - 1) * 30)

            logger.info("Fetching generation from %s to %s", start_time, end_time)
            raw_generation = fetch_elexon_generation_data(start_time, end_time)

            if raw_generation is not None and len(raw_generation) > 0:
                logger.info("Received %d generation records", len(raw_generation))
                transformed = transform_generation_data(raw_generation)
                generation_success = load_generation_data_to_db(db_connection, transformed)
                logger.info("Generation: %s", "✓ SUCCESS" if generation_success else "✗ FAILED")
            else:
                logger.warning("No generation data from API")

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Generation data error: %s", e, exc_info=True)

        # ---- Price Data ----
        try:
            logger.info("[2/2] Processing Price Data")
            fetch_date = datetime.now()
            logger.info("Fetching price data for %s", fetch_date.date())
            raw_price = fetch_elexon_price_data(fetch_date)

            if raw_price is not None:
                parsed_price = parse_elexon_price_data(raw_price)
                logger.info("Parsed %d price records", len(parsed_price))
                transformed_price = update_price_column_names(parsed_price)
                price_success = load_price_data_to_db(db_connection, transformed_price)
                logger.info("Price: %s", "✓ SUCCESS" if price_success else "✗ FAILED")
            else:
                logger.warning("No price data from API")

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Price data error: %s", e, exc_info=True)

        # Return result
        if generation_success or price_success:
            msg = f"Generation: {generation_success}, Price: {price_success}"
            logger.info("✓ %s", msg)
            return True, msg
        else:
            logger.warning("No data processed")
            return True, "No new data available"

    except ImportError as e:
        logger.error("Import error: %s", e, exc_info=True)
        return False, f"Import error: {str(e)}"
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Elexon pipeline error: %s", e, exc_info=True)
        return False, f"Pipeline error: {str(e)}"


# ============================================================
# MAIN HANDLER
# ============================================================

def lambda_handler(_event, _context) -> dict:
    """
    Main Lambda handler - runs Carbon and Elexon pipelines.
    """
    try:
        logger.info("=" * 60)
        logger.info("STARTING 30-MINUTE ETL PIPELINE")
        logger.info("=" * 60)

        # pylint: disable=import-outside-toplevel
        from carbon_pipeline.load_carbon import (
            connect_to_database,
            get_secrets,
            load_secrets_to_env
        )

        # Connect to database
        secrets = get_secrets()
        load_secrets_to_env(secrets)
        db_connection = connect_to_database()
        if not db_connection:
            raise ConnectionError("Failed to establish database connection")

        # Run both pipelines
        carbon_success, carbon_msg = process_carbon_data(db_connection)
        logger.info("Carbon: %s", carbon_msg)

        elexon_success, elexon_msg = process_elexon_data(db_connection)
        logger.info("Elexon: %s", elexon_msg)

        # Close connection
        db_connection.close()

        # Summary
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("Carbon:  %s", "✓ SUCCESS" if carbon_success else "✗ FAILED")
        logger.info("Elexon:  %s", "✓ SUCCESS" if elexon_success else "✗ FAILED")
        logger.info("=" * 60)

        # Return result
        if carbon_success and elexon_success:
            return {'statusCode': 200, 'body': 'All pipelines completed successfully'}
        if carbon_success or elexon_success:
            return {'statusCode': 200, 'body': f'Partial success - Carbon: {carbon_success}, Elexon: {elexon_success}'}
        return {'statusCode': 500, 'body': 'All pipelines failed'}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Critical error: %s", e, exc_info=True)
        return {'statusCode': 500, 'body': f'Pipeline failed: {str(e)}'}


if __name__ == "__main__":
    print("Running 30-Minute ETL Pipeline locally...")
    print("=" * 60)

    mock_event = {}
    mock_context = type('Context', (), {
        'function_name': 'test-30min-pipeline',
        'aws_request_id': 'local-test'
    })()

    result = lambda_handler(mock_event, mock_context)

    print("=" * 60)
    print(f"Status: {result['statusCode']}")
    print(f"Body: {result['body']}")
    print("=" * 60)
