"""
Lambda handler for Elexon pipeline.
Runs every 30 minutes to fetch generation and pricing data.
Fetches last few hours to keep data current and handle any gaps.
"""
import logging
from datetime import datetime, timedelta

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_last_generation_datetime(connection):
    """
    Get the most recent settlement datetime that has generation data.
    Returns None if no data exists (first run).
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
            logger.info(f"Last generation data: {settlement_date} period {settlement_period}")
            return settlement_date, settlement_period
        else:
            logger.info("No existing generation data found - this is the first run")
            return None, None
            
    except Exception as e:
        logger.error(f"Error getting last generation datetime: {e}")
        return None, None

def get_last_price_datetime(connection):
    """
    Get the most recent settlement datetime that has price data.
    Returns None if no data exists (first run).
    """
    try:
        cursor = connection.cursor()
        
        query = """
            SELECT s.settlement_date, s.settlement_period
            FROM system_price sp
            JOIN settlements s ON sp.settlement_id = s.settlement_id
            ORDER BY s.settlement_date DESC, s.settlement_period DESC
            LIMIT 1;
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            settlement_date, settlement_period = result
            logger.info(f"Last price data: {settlement_date} period {settlement_period}")
            return settlement_date, settlement_period
        else:
            logger.info("No existing price data found - this is the first run")
            return None, None
            
    except Exception as e:
        logger.error(f"Error getting last price datetime: {e}")
        return None, None

def calculate_fetch_window(last_date, last_period, data_type="generation"):
    """
    Calculate the start and end datetime for fetching data.
    
    Args:
        last_date: Last settlement date in DB (or None)
        last_period: Last settlement period in DB (or None)
        data_type: Type of data being fetched (for logging)
        
    Returns:
        start_time, end_time (datetime objects)
    """
    end_time = datetime.now()
    
    if last_date is None:
        # First run - fetch last 7 days
        start_time = end_time - timedelta(days=7)
        logger.info(f"{data_type}: First run - fetching last 7 days")
    else:
        # Always fetch last 3 hours to keep data fresh and handle updates
        start_time = end_time - timedelta(hours=3)
        logger.info(f"{data_type}: Fetching last 3 hours to update/fill data")
    
    logger.info(f"{data_type} fetch window: {start_time} to {end_time}")
    
    return start_time, end_time

def lambda_handler(event, context):
    """
    Main Lambda handler for Elexon pipeline.
    Fetches generation and pricing data from Elexon API and loads to RDS.
    """
    try:
        logger.info("Starting Elexon ETL pipeline")
        
        # Import elexon pipeline modules
        from extract_elexon import (
            fetch_elexon_generation_data,
            fetch_elexon_price_data,
            parse_elexon_price_data
        )
        from transform_elexon import (
            transform_generation_data,
            update_price_column_names
        )
        from load_elexon import (
            get_db_connection,
            load_generation_data_to_db,
            load_price_data_to_db
        )
        
        # Get database connection
        db_connection = get_db_connection()
        if not db_connection:
            raise Exception("Failed to establish database connection")
        
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
            start_time, end_time = calculate_fetch_window(
                last_gen_date, 
                last_gen_period,
                "Generation"
            )
            
            # Extract generation data
            logger.info(f"Fetching generation data from Elexon API...")
            raw_generation = fetch_elexon_generation_data(start_time, end_time)
            
            if raw_generation is not None and len(raw_generation) > 0:
                logger.info(f"Received {len(raw_generation)} generation records")
                
                # Transform
                transformed_generation = transform_generation_data(raw_generation)
                logger.info(f"Transformed to {len(transformed_generation)} records")
                
                # Load to database
                generation_success = load_generation_data_to_db(
                    db_connection, 
                    transformed_generation
                )
                logger.info(f"Generation data load: {'SUCCESS' if generation_success else 'FAILED'}")
            else:
                logger.warning("No generation data returned from API")
                
        except Exception as e:
            logger.error(f"Error processing generation data: {e}", exc_info=True)
        
        # ============================================================
        # PROCESS PRICE DATA
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("Processing Elexon Price Data")
            logger.info("=" * 60)
            
            # Get last price datetime from RDS
            last_price_date, last_price_period = get_last_price_datetime(db_connection)
            
            # Price API fetches by day, so use the date from the time window
            fetch_date = datetime.now() if last_price_date is None else datetime.now()
            
            logger.info(f"Fetching price data for date: {fetch_date.date()}")
            raw_price = fetch_elexon_price_data(fetch_date)
            
            if raw_price is not None:
                parsed_price = parse_elexon_price_data(raw_price)
                logger.info(f"Parsed {len(parsed_price)} price records")
                
                transformed_price = update_price_column_names(parsed_price)
                
                price_success = load_price_data_to_db(db_connection, transformed_price)
                logger.info(f"Price data load: {'SUCCESS' if price_success else 'FAILED'}")
            else:
                logger.warning("No price data returned from API")
                
        except Exception as e:
            logger.error(f"Error processing price data: {e}", exc_info=True)
        
        # Close connection
        db_connection.close()
        
        # ============================================================
        # RETURN RESULTS
        # ============================================================
        logger.info("=" * 60)
        if generation_success or price_success:
            result_msg = f"Pipeline completed - Generation: {generation_success}, Price: {price_success}"
            logger.info(f"✓ {result_msg}")
            return {
                'statusCode': 200,
                'body': result_msg
            }
        else:
            logger.warning("No data was successfully processed")
            return {
                'statusCode': 200,
                'body': 'No new data available or all loads failed'
            }
        
    except ImportError as e:
        logger.error(f"Import error - check that all modules are in deployment package: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Import error: {str(e)}'
        }
    
    except Exception as e:
        logger.error(f"Critical error in Elexon pipeline: {e}", exc_info=True)
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