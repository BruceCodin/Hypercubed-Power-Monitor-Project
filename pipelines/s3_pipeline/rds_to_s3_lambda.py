from extract_from_rds import get_historical_power_cut_data
from load_to_s3 import upload_data_to_s3

import logging

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Main Lambda function handler."""

    # Fetch historical power cut data
    logger.info("Fetching historical power cut data from RDS...")
    data = get_historical_power_cut_data()
    logger.info("Data fetched successfully.")

    # Upload data to S3
    logger.info("Uploading data to S3...")
    upload_data_to_s3(data)
    logger.info("Data uploaded to S3 successfully.")

    return {
        'statusCode': 200,
        'body': 'Power cut data successfully extracted from RDS and uploaded to S3.'
    }


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    response = lambda_handler(None, None)
    print(response)
