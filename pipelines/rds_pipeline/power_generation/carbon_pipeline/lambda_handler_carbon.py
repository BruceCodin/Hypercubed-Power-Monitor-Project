"""
Lambda handler for Carbon Intensity pipeline.
Runs every 30 minutes to fetch and load carbon intensity data.
Fetches from last known datetime in RDS to now for automatic gap filling.
"""
import logging
from datetime import datetime, timedelta

# Configure logging
logger = logging.getLogger()
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
            logger.info(f"Last carbon data: {settlement_date} period {settlement_period}")
            return settlement_date, settlement_period
        else:
            logger.info("No existing carbon data found - this is the first run")
            return None, None
            
    except Exception as e:
        logger.error(f"Error getting last carbon datetime: {e}")
        return None, None

def calculate_fetch_window(last_date, last_period):
    end_time = datetime.now() + timedelta(minutes=5)
    start_time = end_time - timedelta(hours=3)  # Last 3 hours
    logger.info(f"Fetching last 3 hours: {start_time} to {end_time}")
    return start_time, end_time

def lambda_handler(event, context):
    """
    Main Lambda handler for Carbon Intensity pipeline.
    Fetches data from last known point in RDS to now.
    """
    try:
        logger.info("Starting Carbon Intensity ETL pipeline")
        
        # Import carbon pipeline modules
        from extract_carbon import fetch_carbon_intensity_data
        from transform_carbon import transform_carbon_data
        from load_carbon import get_db_connection, load_carbon_data_to_db
        
        # Get database connection
        db_connection = get_db_connection()
        if not db_connection:
            raise Exception("Failed to establish database connection")
        
        # Get last datetime from RDS
        last_date, last_period = get_last_carbon_datetime(db_connection)
        
        # Calculate time window for fetching
        start_time, end_time = calculate_fetch_window(last_date, last_period)
        
        # Check if there's anything to fetch
        if start_time >= end_time:
            logger.info("Already up to date - no new data to fetch")
            db_connection.close()
            return {
                'statusCode': 200,
                'body': 'Already up to date'
            }
        
        # Extract carbon intensity data
        logger.info(f"Extracting carbon intensity data from {start_time} to {end_time}")
        raw_carbon = fetch_carbon_intensity_data(start_time, end_time)
        
        if raw_carbon is None or len(raw_carbon) == 0:
            logger.warning("No carbon intensity data returned from API")
            db_connection.close()
            return {
                'statusCode': 200,
                'body': 'No new carbon data available from API'
            }
        
        logger.info(f"Received {len(raw_carbon)} records from Carbon API")
        
        # Transform data
        logger.info("Transforming carbon data")
        transformed_carbon = transform_carbon_data(raw_carbon)
        logger.info(f"Transformed to {len(transformed_carbon)} records")
        
        # Load to database (ON CONFLICT will handle any duplicates)
        logger.info("Loading carbon data to database")
        success = load_carbon_data_to_db(db_connection, transformed_carbon)
        
        # Close connection
        db_connection.close()
        
        if success:
            logger.info(f"Carbon pipeline completed successfully - processed {len(transformed_carbon)} records")
            return {
                'statusCode': 200,
                'body': f'Successfully processed {len(transformed_carbon)} carbon records'
            }
        else:
            logger.error("Failed to load carbon data to database")
            return {
                'statusCode': 500,
                'body': 'Failed to load carbon data'
            }
        
    except ImportError as e:
        logger.error(f"Import error - check that all modules are in deployment package: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Import error: {str(e)}'
        }
    
    except Exception as e:
        logger.error(f"Critical error in Carbon pipeline: {e}", exc_info=True)
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