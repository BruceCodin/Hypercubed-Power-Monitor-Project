from extract_from_rds import (
    get_historical_power_cut_data)
import logging

import pandas as pd
import awswrangler as wr

POWER_CUT_S3_PATH = "s3://c20-power-monitor-s3/power_cuts/"


def upload_data_to_s3(data: pd.DataFrame) -> None:
    """Uploads data to an S3 bucket time-partitioned by the

    Args:
        data (pd.DataFrame): DataFrame containing historical power cut data
    """

    alerts_df = data.copy()

    alerts_df['year'] = alerts_df['recording_time'].dt.year
    alerts_df['month'] = alerts_df['recording_time'].dt.month
    alerts_df['day'] = alerts_df['recording_time'].dt.day

    wr.s3.to_parquet(
        df=alerts_df,
        path=POWER_CUT_S3_PATH,
        dataset=True,
        mode="overwrite",
        partition_cols=['year', 'month', 'day']
    )


if __name__ == "__main__":

    from extract_from_rds import (
        get_historical_power_cut_data)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    data = get_historical_power_cut_data()
    upload_data_to_s3(data)
