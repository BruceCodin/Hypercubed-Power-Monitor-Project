import pandas as pd
import awswrangler as wr
import logging

POWER_CUT_S3_PATH = "s3://c20-power-monitor-s3/power_cuts/"
logger = logging.getLogger(__name__)


def upload_data_to_s3(data: pd.DataFrame) -> None:
    """Uploads data to an S3 bucket time-partitioned by the

    Args:
        data (pd.DataFrame): DataFrame containing historical power cut data
    """

    alerts_df = data.copy()

    alerts_df['year'] = pd.to_datetime(alerts_df['recording_time']).dt.year
    alerts_df['month'] = pd.to_datetime(alerts_df['recording_time']).dt.month
    alerts_df['day'] = pd.to_datetime(alerts_df['recording_time']).dt.day

    logger.info(f"Uploading data to S3 at {POWER_CUT_S3_PATH}...")
    wr.s3.to_parquet(
        df=alerts_df,
        path=POWER_CUT_S3_PATH,
        dataset=True,
        mode="overwrite",
        partition_cols=['year', 'month', 'day']
    )
    logger.info("Data uploaded to S3 successfully.")


if __name__ == "__main__":

    from extract_from_rds import (
        get_historical_power_cut_data)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Extracting historical power cut data from RDS...")
    data = get_historical_power_cut_data()
    print(data.head(10))  # Print first 10 records
    print(data.info())
    logging.info("Extraction complete")

    logging.info("Uploading data to S3...")
    upload_data_to_s3(data)
    logging.info("Upload complete.")
