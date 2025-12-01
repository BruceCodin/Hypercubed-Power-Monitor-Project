"""Lambda function to extract power cut data from RDS and upload to S3."""

import logging

from extract_cuts_from_rds import get_historical_power_cut_data
from load_cuts_to_s3 import upload_data_to_s3


# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Main Lambda function handler."""

    try:
        # Fetch historical power cut data
        logger.info("--- START FETCHING DATA FROM RDS ---")
        data = get_historical_power_cut_data()
        logger.info("--- DATA FETCHED SUCCESSFULLY FROM RDS ---")
        # Upload data to S3
        logger.info("--- START UPLOADING DATA TO S3 ---")
        upload_data_to_s3(data)
        logger.info("--- DATA UPLOADED SUCCESSFULLY TO S3 ---")

    except Exception as e:
        logger.error("An error occurred: %s", e)
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }

    return {
        'statusCode': 200,
        'body': 'Power cut data successfully extracted from RDS and uploaded to S3.'
    }


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    response = lambda_handler(None, None)
    print(response)
