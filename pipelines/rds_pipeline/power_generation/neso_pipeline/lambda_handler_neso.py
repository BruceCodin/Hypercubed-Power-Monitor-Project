"""
Lambda handler for NESO demand data pipeline.
Runs once daily to fetch and load power demand data.
"""
import logging
from datetime import datetime
import psycopg2
from extract_neso import fetch_neso_demand_data, parse_neso_demand_data
from transform_neso import transform_neso_demand_data
from load_neso import (
    connect_to_database,
    load_neso_demand_data_to_db,
    get_secrets,
    load_secrets_to_env
)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# NESO API Resource ID
RECENT_RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"

def get_last_data_timestamp(db_connection: psycopg2.extensions.connection) -> datetime:
    """
    Get max settlement date and period from the NESO demand data table.

    Args:
        db_connection (psycopg2.extensions.connection): Database connection object.
    
    Returns:
        settlement_date(datetime): Latest settlement datetime in the table.
        settlement_period (int): Latest settlement period in the table.
    """

    query = """
            SELECT s.settlement_date, s.settlement_period
                FROM settlements s
                JOIN recent_demand rd ON rd.settlement_id = s.settlement_id
                WHERE rd.national_demand > 0
                ORDER BY s.settlement_date DESC, s.settlement_period DESC
                LIMIT 1;
            """
    try:
        with db_connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                settlement_date, settlement_period = result
                return settlement_date, settlement_period
            else:
                return None, None
    except Exception as e:
        logger.error(f"Error fetching last data timestamp: {e}")
        return None, None

def lambda_handler(_event, _context) -> dict:  # pylint: disable=unused-argument
    """
    Main Lambda handler for NESO demand pipeline.
    - Loads recent demand data from last known timestamp to now.

    Args:
        _event: AWS Lambda event object (unused)
        _context: AWS Lambda context object (unused)

    Returns:
        dict: Response with statusCode and body
    """
    try:
        logger.info("=" * 60)
        logger.info("STARTING NESO DEMAND ETL PIPELINE")
        logger.info("Timestamp: %s", datetime.now())
        logger.info("=" * 60)

        # Get database connection
        secrets = get_secrets()
        load_secrets_to_env(secrets)
        db_connection = connect_to_database()
        if not db_connection:
            raise ConnectionError("Failed to establish database connection")

        recent_success = False
        # ============================================================
        # LOAD RECENT DEMAND DATA (DAILY)
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("RECENT DEMAND DATA")
            logger.info("=" * 60)

            # Fetch recent data
            logger.info("Fetching recent demand data...")
            last_date, last_period = get_last_data_timestamp(db_connection)
            logger.info(f"Last settlement date: {last_date}, Last settlement period: {last_period}")

            # If no data exists yet, start from today
            if last_date is None:
                last_date_str = datetime.now().strftime("%Y-%m-%d")
                last_period = 0
            else:
                last_date_str = last_date.strftime("%Y-%m-%d")

            raw_recent = fetch_neso_demand_data(last_date_str, last_period)

            if raw_recent is None:
                logger.error("Failed to fetch recent demand data")
            else:
                # Parse and transform
                parsed_recent = parse_neso_demand_data(raw_recent)
                logger.info("Parsed %d recent records", len(parsed_recent))

                transformed_recent = transform_neso_demand_data(parsed_recent)
                logger.info("Transformed to %d records", len(transformed_recent))

                # Load to database
                recent_success = load_neso_demand_data_to_db(
                    db_connection,
                    transformed_recent
                )

                if recent_success:                    
                    logger.info(
                        "✓ Recent data loaded successfully - %d records",
                        len(transformed_recent)
                    )
                else:
                    logger.error("✗ Recent data load failed")

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Error processing recent demand data: %s", e, exc_info=True
            )

        # Close connection
        db_connection.close()

        # Return results
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info("Recent:   %s", '✓' if recent_success else '✗')
        logger.info("=" * 60)

        if recent_success:
            return {
                'statusCode': 200,
                'body': 'Recent demand data loaded successfully'
            }
        return {
            'statusCode': 500,
            'body': 'NESO pipeline failed'
        }

    except ImportError as e:
        logger.error(
            "Import error - check modules in deployment package: %s",
            e, exc_info=True
        )
        return {
            'statusCode': 500,
            'body': f'Import error: {str(e)}'
        }

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Critical error in NESO pipeline: %s", e, exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Pipeline failed: {str(e)}'
        }

if __name__ == '__main__':
    lambda_handler(None, None)
