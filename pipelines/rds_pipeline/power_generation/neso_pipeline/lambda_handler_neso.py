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

# NESO API Resource IDs
HISTORICAL_RESOURCE_ID = "b2bde559-3455-4021-b179-dfe60c0337b0"
RECENT_RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"

def check_historic_data_exists(connection: psycopg2.extensions.connection) -> bool:
    """
    Check if historic_demand table has any data.
    Returns True if data exists, False if empty.

    Args:
        connection: psycopg2 database connection

    Returns:
        bool: True if data exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM historic_demand;")
        count = cursor.fetchone()[0]
        cursor.close()

        logger.info("Historic demand table has %d records", count)
        return count > 0

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error checking historic data: %s", e)
        return False

def lambda_handler(_event, _context) -> dict:  # pylint: disable=unused-argument
    """
    Main Lambda handler for NESO demand pipeline.
    - Loads historic data once (if not already loaded)
    - Always loads recent demand data (daily updates)

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

        historic_success = False
        recent_success = False

        # ============================================================
        # LOAD HISTORIC DEMAND DATA (ONE-TIME ONLY)
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("HISTORIC DEMAND DATA")
            logger.info("=" * 60)

            # Check if we already have historic data
            has_historic_data = check_historic_data_exists(db_connection)

            if has_historic_data:
                logger.info("✓ Historic data already exists - skipping")
                historic_success = True
            else:
                logger.info("No historic data found - loading now...")

                # Fetch historic data
                raw_historic = fetch_neso_demand_data(HISTORICAL_RESOURCE_ID)
                if raw_historic is None:
                    logger.error("Failed to fetch historic demand data")
                else:
                    # Parse and transform
                    parsed_historic = parse_neso_demand_data(raw_historic)
                    logger.info("Parsed %d historic records", len(parsed_historic))

                    transformed_historic = transform_neso_demand_data(parsed_historic)
                    logger.info("Transformed to %d records", len(transformed_historic))

                    # Load to database
                    historic_success = load_neso_demand_data_to_db(
                        db_connection,
                        transformed_historic,
                        'historic_demand'
                    )

                    if historic_success:
                        logger.info(
                            "✓ Historic data loaded successfully - %d records",
                            len(transformed_historic)
                        )
                    else:
                        logger.error("✗ Historic data load failed")

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Error processing historic demand data: %s", e, exc_info=True
            )

        # ============================================================
        # LOAD RECENT DEMAND DATA (DAILY)
        # ============================================================
        try:
            logger.info("=" * 60)
            logger.info("RECENT DEMAND DATA")
            logger.info("=" * 60)

            # Fetch recent data
            logger.info("Fetching recent demand data...")
            raw_recent = fetch_neso_demand_data(RECENT_RESOURCE_ID)

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
                    transformed_recent,
                    'recent_demand'
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
        logger.info("Historic: %s", '✓' if historic_success else '✗')
        logger.info("Recent:   %s", '✓' if recent_success else '✗')
        logger.info("=" * 60)

        if historic_success and recent_success:
            return {
                'statusCode': 200,
                'body': 'NESO demand pipeline completed successfully'
            }
        if recent_success:
            return {
                'statusCode': 200,
                'body': 'Recent demand data loaded (historic already exists)'
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


if __name__ == "__main__":
    """Allow running the handler locally for testing"""
    print("Running NESO Demand Lambda Handler locally...")
    print("=" * 60)

    # Mock Lambda event and context
    mock_event = {}
    mock_context = type('Context', (), {
        'function_name': 'test-neso-pipeline',
        'aws_request_id': 'local-test'
    })()

    # Run the handler
    result = lambda_handler(mock_event, mock_context)

    print("=" * 60)
    print(f"Status: {result['statusCode']}")
    print(f"Body: {result['body']}")
    print("=" * 60)
