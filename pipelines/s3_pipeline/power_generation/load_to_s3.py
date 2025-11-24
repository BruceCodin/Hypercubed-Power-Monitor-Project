"""Module to load historical power cut data to S3 bucket."""

import logging

import pandas as pd
import awswrangler as wr

POWER_GENERATION_S3_PATH = "s3://c20-power-monitor-s3/power_generation/"
CARBON_S3_PATH = POWER_GENERATION_S3_PATH + "carbon_intensity/"
ENERGY_GENERATION_S3_PATH = POWER_GENERATION_S3_PATH + "energy_generation/"

logger = logging.getLogger(__name__)


def prepare_carbon_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Prepares historical carbon intensity data for S3 upload.

    Args:
        raw_data (pd.DataFrame): Raw DataFrame from RDS extraction

    Returns:
        pd.DataFrame: Processed DataFrame ready for S3 upload
    """

    carbon_df = raw_data.copy()

    # Convert settlement_date to datetime
    carbon_df['settlement_date'] = pd.to_datetime(carbon_df['settlement_date'])

    carbon_df['year'] = carbon_df['settlement_date'].dt.year
    carbon_df['month'] = carbon_df['settlement_date'].dt.month
    carbon_df['day'] = carbon_df['settlement_date'].dt.day

    return carbon_df


def prepare_energy_generation_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Prepares historical power generation data for S3 upload.

    Args:
        raw_data (pd.DataFrame): Raw DataFrame from RDS extraction

    Returns:
        pd.DataFrame: Processed DataFrame ready for S3 upload
    """

    generation_df = raw_data.copy()

    # Convert generation_date to datetime
    generation_df['settlement_date'] = pd.to_datetime(
        generation_df['settlement_date'])

    generation_df['year'] = generation_df['settlement_date'].dt.year
    generation_df['month'] = generation_df['settlement_date'].dt.month
    generation_df['day'] = generation_df['settlement_date'].dt.day

    return generation_df


def upload_carbon_data_to_s3(carbon_data: pd.DataFrame) -> None:
    """Uploads data to an S3 bucket time-partitioned by the

    Args:
        carbon_data (pd.DataFrame): DataFrame containing historical carbon intensity data
    """

    logger.info("Uploading carbon data to S3 at %s...", CARBON_S3_PATH)
    wr.s3.to_parquet(
        df=carbon_data,
        path=CARBON_S3_PATH,
        dataset=True,
        mode="overwrite",
        partition_cols=['year', 'month', 'day']
    )
    logger.info("Carbon data uploaded to S3 successfully.")


def upload_energy_generation_data_to_s3(generation_data: pd.DataFrame) -> None:
    """Uploads data to an S3 bucket time-partitioned by the

    Args:
        generation_data (pd.DataFrame): DataFrame containing historical power generation data
    """

    logger.info("Uploading energy generation data to S3 at %s...",
                ENERGY_GENERATION_S3_PATH)
    wr.s3.to_parquet(
        df=generation_data,
        path=ENERGY_GENERATION_S3_PATH,
        dataset=True,
        mode="overwrite",
        partition_cols=['year', 'month', 'day']
    )
    logger.info("Energy generation data uploaded to S3 successfully.")


if __name__ == "__main__":

    from extract_from_rds import (
        get_secrets,
        load_secrets_to_env,
        connect_to_database,
        get_historical_carbon_data,
        get_historical_power_generation_data
    )

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    logger.info("Starting extraction from RDS...")
    secrets = get_secrets()
    load_secrets_to_env(secrets)
    conn = connect_to_database()
    logger.info("Connected to RDS successfully.")

    logger.info("Extracting historical data from RDS...")
    raw_carbon_data = get_historical_carbon_data(conn)
    raw_generation_data = get_historical_power_generation_data(conn)
    conn.close()

    logger.info("Data extraction complete. Connection to RDS closed.")

    logger.info("Preparing data for S3 upload...")
    clean_carbon_data = prepare_carbon_data(raw_carbon_data)
    clean_generation_data = prepare_energy_generation_data(raw_generation_data)
    logger.info("Data preparation complete.")

    logger.info("Uploading energy generation data to S3...")
    upload_energy_generation_data_to_s3(clean_generation_data)
    logger.info("Uploading carbon data to S3...")
    upload_carbon_data_to_s3(clean_carbon_data)

    logger.info("All data uploaded to S3 successfully.")
