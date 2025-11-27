"""
Lambda handler for Elexon pipeline.
Runs every 30 minutes to fetch generation and pricing data.
Fetches last few hours to keep data current and handle any gaps.
"""
import logging
from datetime import datetime, timedelta
import psycopg2
from extract_elexon import (
    fetch_elexon_generation_data,
    fetch_elexon_price_data,
    parse_elexon_price_data
)
from transform_elexon import transform_generation_data, update_price_column_names
from load_elexon import (
    connect_to_database,
    load_generation_data_to_db,
    load_price_data_to_db,
    get_secrets,
    load_secrets_to_env
)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_last_generation_datetime(connection):
    """
    Get the most recent settlement datetime that has generation data.
    Returns None if no data exists (first run).

    Args:
        connection: psycopg2 database connection object

    Returns:
        tuple: (settlement_date, settlement_period) or (None, None) if no data exists
    """
    try:
        cursor = connection.cursor()

        query = """
            SELECT s.settlement_date, s.settlement_period
            FROM generation g
            JOIN settlements s ON g.settlement_id = s.settlement_id
            ORDER BY s.settlement_date DESC, s.settlement_period DESC
            LIMIT 1;
        """

        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            settlement_date, settlement_period = result
            logger.info("Last generation data: %s period %s", settlement_date, settlement_period)
            return settlement_date, settlement_period

        logger.info("No existing generation data found - this is the first run")
        return None, None

    except psycopg2.Error as e:
        logger.error("Database error getting last generation datetime: %s", e)
        return None, None


def calculate_fetch_window(last_date, last_period):
    """
    Calculate the start and end datetime for fetching generation data.
    Fetches from last known settlement period to now, or last 7 days on first run.

    Args:
        last_date: Last settlement date in DB (or None)
        last_period: Last settlement period in DB (or None)

    Returns:
        tuple: (start_time, end_time) datetime objects
    """
    end_time = datetime.now() + timedelta(minutes=5)

    if last_date is None:
        # First run - fetch last 7 days
        start_time = end_time - timedelta(days=7)
        logger.info("First run - fetching last 7 days: %s to %s", start_time, end_time)
    else:
        # Calculate exact datetime from last settlement period
        # Each period is 30 minutes, periods 1-48 represent 00:00-23:30
        start_time = datetime.combine(last_date, datetime.min.time())
        start_time += timedelta(minutes=(last_period - 1) * 30)
        logger.info(
            "Fetching from last settlement: %s (period %s) to %s",
            start_time, last_period, end_time
        )

    return start_time, end_time

def lambda_handler(event, context):  # pylint: disable=unused-argument
    """
    Main Lambda handler for Elexon pipeline.
    Fetches generation and pricing data from Elexon API and loads to RDS.

    Args:
        event: AWS Lambda event object
        context: AWS Lambda context object

    Returns:
        dict: Response with statusCode and body
    """
    try:
        logger.info("Starting Elexon ETL pipeline")

        # Get database connection
        secrets = get_secrets()
        load_secrets_to_env(secrets)
        db_connection = connect_to_database()
        if not db_connection:
            raise ConnectionError("Failed to establish database connection")

        generation_success = False
        price_success = False

        # ============================================================
        # PROCESS GENERATION DATA
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("Processing Elexon Generation Data")
            logger.info("=" * 60)

            # Get last generation datetime from RDS
            last_gen_date, last_gen_period = get_last_generation_datetime(db_connection)

            # Calculate time window
            start_time, end_time = calculate_fetch_window(last_gen_date, last_gen_period)

            # Extract generation data
            logger.info("Fetching generation data from Elexon API...")
            raw_generation = fetch_elexon_generation_data(start_time, end_time)

            if raw_generation is not None and len(raw_generation) > 0:
                logger.info("Received %d generation records", len(raw_generation))

                # Transform
                transformed_generation = transform_generation_data(raw_generation)
                logger.info("Transformed to %d records", len(transformed_generation))

                # Load to database
                generation_success = load_generation_data_to_db(
                    db_connection,
                    transformed_generation
                )
                logger.info("Generation data load: %s",
                          'SUCCESS' if generation_success else 'FAILED')
            else:
                logger.warning("No generation data returned from API")

        except (psycopg2.Error, ConnectionError) as e:
            logger.error("Database error processing generation data: %s", e, exc_info=True)
        except (KeyError, ValueError) as e:
            logger.error("Data processing error in generation data: %s", e, exc_info=True)

        # ============================================================
        # PROCESS PRICE DATA
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("Processing Elexon Price Data")
            logger.info("=" * 60)

            # Price API fetches by day, so always fetch current day
            fetch_date = datetime.now()

            logger.info("Fetching price data for date: %s", fetch_date.date())
            raw_price = fetch_elexon_price_data(fetch_date)

            if raw_price is not None:
                parsed_price = parse_elexon_price_data(raw_price)
                logger.info("Parsed %d price records", len(parsed_price))

                transformed_price = update_price_column_names(parsed_price)

                price_success = load_price_data_to_db(db_connection, transformed_price)
                logger.info("Price data load: %s",
                          'SUCCESS' if price_success else 'FAILED')
            else:
                logger.warning("No price data returned from API")

        except (psycopg2.Error, ConnectionError) as e:
            logger.error("Database error processing price data: %s", e, exc_info=True)
        except (KeyError, ValueError) as e:
            logger.error("Data processing error in price data: %s", e, exc_info=True)

        # Close connection
        db_connection.close()

        # ============================================================
        # RETURN RESULTS
        # ============================================================
        logger.info("=" * 60)
        if generation_success or price_success:
            result_msg = (
                f"Pipeline completed - Generation: {generation_success}, "
                f"Price: {price_success}"
            )
            logger.info("✓ %s", result_msg)
            return {
                'statusCode': 200,
                'body': result_msg
            }

        logger.warning("No data was successfully processed")
        return {
            'statusCode': 200,
            'body': 'No new data available or all loads failed'
        }

    except ImportError as e:
        logger.error("Import error - check deployment package: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Import error: {str(e)}'
        }
    except (ConnectionError, psycopg2.Error) as e:
        logger.error("Database connection error in Elexon pipeline: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Database error: {str(e)}'
        }
    except Exception as e:  # pylint: disable=broad-except
        # Broad exception needed to catch any unexpected errors and return proper Lambda response
        logger.error("Critical error in Elexon pipeline: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Pipeline failed: {str(e)}'
        }


if __name__ == "__main__":
    """Allow running the handler locally for testing"""
    print("Running Elexon Lambda Handler locally...")
    print("=" * 60)

    # Mock Lambda event and context
    mock_event = {}
    mock_context = type('Context', (), {
        'function_name': 'test-elexon-pipeline',
        'aws_request_id': 'local-test'
    })()

    # Run the handler
    result = lambda_handler(mock_event, mock_context)

    print("=" * 60)
    print(f"Status: {result['statusCode']}")
    print(f"Body: {result['body']}")
    print("=" * 60)
