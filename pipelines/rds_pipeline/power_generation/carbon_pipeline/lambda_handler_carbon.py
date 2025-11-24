"""
Lambda handler for Carbon Intensity pipeline.
Runs every 30 minutes to fetch and load carbon intensity data.
Fetches from last known datetime in RDS to now for automatic gap filling.
"""
import logging
from datetime import datetime, timedelta

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_last_carbon_datetime(connection):
    """
    Get the most recent settlement datetime that has carbon data.
    Returns None if no data exists (first run).
    """
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

        logger.info("No existing carbon data found - this is the first run")
        return None, None

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error getting last carbon datetime: %s", e)
        return None, None

def calculate_fetch_window(last_date: datetime.date, last_period: int) -> tuple:
    """
    Calculate the start and end datetime for fetching data.
    Fetches from last known settlement period to now, or last 7 days on first run.

    Args:
        last_date: Last settlement date in DB (or None for first run)
        last_period: Last settlement period in DB (or None for first run)

    Returns:
        start_time, end_time (datetime objects)
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

def lambda_handler(event: dict, context: dict) -> dict:  # pylint: disable=unused-argument
    """
    Main Lambda handler for Carbon Intensity pipeline.
    Fetches data from last known point in RDS to now.

    Args:
        event: AWS Lambda event object (unused)
        context: AWS Lambda context object (unused)

    Returns:
        dict: Response with statusCode and body
    """
    try:
        logger.info("=" * 60)
        logger.info("STARTING CARBON INTENSITY ETL PIPELINE")
        logger.info("Timestamp: %s", datetime.now())
        logger.info("=" * 60)

        # Import carbon pipeline modules
        # pylint: disable=import-outside-toplevel
        from extract_carbon import fetch_carbon_intensity_data
        from transform_carbon import transform_carbon_data
        from load_carbon import (
            connect_to_database,
            load_carbon_data_to_db,
            get_secrets,
            load_secrets_to_env
        )

        # Get database connection
        secrets = get_secrets()
        load_secrets_to_env(secrets)
        db_connection = connect_to_database()
        if not db_connection:
            raise ConnectionError("Failed to establish database connection")
        
        # Get last datetime from RDS
        last_date, last_period = get_last_carbon_datetime(db_connection)
        
        # Calculate time window for fetching
        start_time, end_time = calculate_fetch_window(last_date, last_period)

        # Extract carbon intensity data
        logger.info("Extracting carbon intensity data from %s to %s", start_time, end_time)
        raw_carbon = fetch_carbon_intensity_data(start_time, end_time)

        if raw_carbon is None or len(raw_carbon) == 0:
            logger.warning("No carbon intensity data returned from API")
            db_connection.close()
            return {
                'statusCode': 200,
                'body': 'No new carbon data available from API'
            }

        logger.info("Received %d records from Carbon API", len(raw_carbon))

        # Transform data
        transformed_carbon = transform_carbon_data(raw_carbon)
        logger.info("Transformed to %d records", len(transformed_carbon))
        
        # Load to database (ON CONFLICT will handle any duplicates)
        success = load_carbon_data_to_db(db_connection, transformed_carbon)
        
        # Close connection
        db_connection.close()

        # Return results
        logger.info("=" * 60)
        if success:
            logger.info("✓ Carbon pipeline completed - %d records", len(transformed_carbon))
            logger.info("=" * 60)
            return {
                'statusCode': 200,
                'body': f'Successfully processed {len(transformed_carbon)} carbon records'
            }

        logger.error("✗ Failed to load carbon data to database")
        logger.info("=" * 60)
        return {
            'statusCode': 500,
            'body': 'Failed to load carbon data'
        }

    except ImportError as e:
        logger.error(
            "Import error - check that all modules are in deployment package: %s",
            e, exc_info=True
        )
        return {
            'statusCode': 500,
            'body': f'Import error: {str(e)}'
        }

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Critical error in Carbon pipeline: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Pipeline failed: {str(e)}'
        }


if __name__ == "__main__":
    """Allow running the handler locally for testing"""
    print("Running Carbon Lambda Handler locally...")
    print("=" * 60)
    
    # Mock Lambda event and context
    mock_event = {}
    mock_context = type('Context', (), {
        'function_name': 'test-carbon-pipeline',
        'aws_request_id': 'local-test'
    })()
    
    # Run the handler
    result = lambda_handler(mock_event, mock_context)
    
    print("=" * 60)
    print(f"Status: {result['statusCode']}")
    print(f"Body: {result['body']}")
    print("=" * 60)