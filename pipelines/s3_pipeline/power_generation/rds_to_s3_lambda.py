"""Lambda function to extract power cut data from RDS and upload to S3."""

import logging

from extract_from_rds import (get_secrets,
                              load_secrets_to_env,
                              connect_to_database,
                              get_historical_power_generation_data,
                              get_historical_carbon_data)
from load_to_s3 import (prepare_carbon_data,
                        prepare_power_cut_data,
                        upload_carbon_data_to_s3,
                        upload_energy_generation_data_to_s3)


# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Main Lambda function handler."""

    try:
        logger.info("Loading secrets from Secrets Manager...")
        secrets = get_secrets()
        logger.info("Secrets loaded successfully.")

        logger.info("Loading secrets into environment variables...")
        load_secrets_to_env(secrets)
        logger.info("Secrets loaded into environment variables.")

        logger.info("Establishing database connection...")
        conn = connect_to_database()
        logger.info("Database connection established.")

        logger.info("Extracting historical data from RDS...")
        raw_carbon_data = get_historical_carbon_data(conn)
        raw_generation_data = get_historical_power_generation_data(conn)
        logger.info("Historical data extraction complete.")
        conn.close()

        logger.info("Preparing data for S3 upload...")
        partitioned_carbon_data = prepare_carbon_data(raw_carbon_data)
        partitioned_generation_data = prepare_power_cut_data(
            raw_generation_data)
        logger.info("Data preparation complete.")

        logger.info("Uploading data to S3...")
        upload_carbon_data_to_s3(partitioned_carbon_data)
        upload_energy_generation_data_to_s3(partitioned_generation_data)
        logger.info("Data upload to S3 complete.")

    except Exception as e:
        logger.error("An error occurred: %s", e)
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }

    return {
        'statusCode': 200,
        'body': 'Power generation data successfully extracted from RDS and uploaded to S3.'
    }


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    response = lambda_handler(None, None)
    print(response)
