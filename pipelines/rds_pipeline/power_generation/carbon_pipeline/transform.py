'''Transform module for Carbon Pipeline.'''
import logging
from datetime import datetime
import pandas as pd
from extract import fetch_carbon_intensity_data
# pylint: disable = logging-fstring-interpolation

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def add_settlement_period(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Add settlement period to carbon intensity DataFrame based on from and to timestamps.
    Each from to interval corresponds to a settlement period.

    Args:
        carbon_df (pd.DataFrame): DataFrame containing carbon intensity 
                                  data with 'from' and 'to' columns.

    Returns:
        pd.DataFrame: DataFrame with added 'settlement_period' column.
    '''
    if not isinstance(carbon_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(carbon_df)}")

    if carbon_df.empty:
        return carbon_df

    if 'from' not in carbon_df.columns or 'to' not in carbon_df.columns:
        raise ValueError("DataFrame must contain 'from' and 'to' columns")

    try:
        settlement_periods = []
        for _, row in carbon_df.iterrows():
            from_time = datetime.strptime(row['from'], "%Y-%m-%dT%H:%MZ")
            # Settlement periods are half-hourly intervals starting from midnight
            settlement_period = (from_time.hour * 2) + (1 if from_time.minute >= 30 else 0) + 1
            settlement_periods.append(settlement_period)

        carbon_df['settlement_period'] = settlement_periods
        logger.info(f"Added settlement periods to {len(carbon_df)} rows")
        return carbon_df
    except ValueError as e:
        logger.error(f"Failed to parse timestamp: {e}")
        raise

def refactor_intensity_column(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Extract columns from intensity dictionary into separate columns.

    Args:
        carbon_df (pd.DataFrame): DataFrame containing carbon intensity
                                  data with 'intensity' column.

    Returns:
        pd.DataFrame: DataFrame with extracted intensity columns.
    '''
    if not isinstance(carbon_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(carbon_df)}")

    if carbon_df.empty:
        return carbon_df

    if 'intensity' not in carbon_df.columns:
        raise ValueError("DataFrame must contain 'intensity' column")

    try:
        intensity_data = carbon_df['intensity'].apply(pd.Series)
        carbon_df = pd.concat([carbon_df.drop(columns=['intensity']), intensity_data], axis=1)
        logger.info(f"Refactored intensity column into {len(intensity_data.columns)} columns")
        return carbon_df
    except Exception as e:
        logger.error(f"Failed to refactor intensity column: {e}")
        raise ValueError(f"Failed to parse intensity column data: {e}") from e

def add_date_column(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Add a date column based on the 'from' timestamp and removes the 'from' and 'to' column.

    Args:
        carbon_df (pd.DataFrame): DataFrame containing carbon intensity data with 'from' column.

    Returns:
        pd.DataFrame: DataFrame with added 'date' column.
    '''
    if not isinstance(carbon_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(carbon_df)}")

    if carbon_df.empty:
        return carbon_df

    if 'from' not in carbon_df.columns:
        raise ValueError("DataFrame must contain 'from' column")

    try:
        carbon_df['date'] = carbon_df['from'].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%MZ").date()
        )
        columns_to_drop = [col for col in ['from', 'to'] if col in carbon_df.columns]
        carbon_df = carbon_df.drop(columns=columns_to_drop)
        logger.info(f"Added date column and dropped {columns_to_drop}")
        return carbon_df
    except ValueError as e:
        logger.error(f"Failed to parse timestamp: {e}")
        raise

def transform_carbon_data(from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:
    '''
    Fetch and transform carbon intensity data for a specific date range.

    Args:
        from_datetime (datetime): Start datetime for data extraction.
        to_datetime (datetime): End datetime for data extraction.

    Returns:
        pd.DataFrame: Transformed carbon intensity data.
    '''
    if not isinstance(from_datetime, datetime):
        raise TypeError(f"from_datetime must be a datetime object, got {type(from_datetime)}")

    if not isinstance(to_datetime, datetime):
        raise TypeError(f"to_datetime must be a datetime object, got {type(to_datetime)}")

    if from_datetime >= to_datetime:
        raise ValueError("from_datetime must be before to_datetime")

    try:
        logger.info(f"Transforming carbon data for {from_datetime} to {to_datetime}")

        carbon_df = fetch_carbon_intensity_data(from_datetime, to_datetime)
        if carbon_df is None or carbon_df.empty:
            logger.warning("No data returned from API")
            return pd.DataFrame()

        carbon_df = refactor_intensity_column(carbon_df)
        carbon_df = add_settlement_period(carbon_df)
        carbon_df = add_date_column(carbon_df)
        carbon_df = update_column_names(carbon_df)

        logger.info(f"Successfully transformed {len(carbon_df)} rows")
        return carbon_df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def update_column_names(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Update column names to match database schema.
    forecast -> intensity_forecast
    actual -> intensity_actual
    index -> carbon_index

    Args:
        carbon_df (pd.DataFrame): DataFrame containing carbon intensity data.

    Returns:
        pd.DataFrame: DataFrame with updated column names.
    '''
    if not isinstance(carbon_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(carbon_df)}")

    if carbon_df.empty:
        return carbon_df

    column_mapping = {
        'forecast': 'intensity_forecast',
        'actual': 'intensity_actual',
        'index': 'carbon_index'
    }

    carbon_df = carbon_df.rename(columns=column_mapping)
    logger.info("Updated column names to match database schema")
    return carbon_df

if __name__ == "__main__":
    # Example usage for refactor_intensity_column
    from_datetime = datetime(2025, 1, 1, 0, 0)
    to_datetime = datetime(2025, 1, 1, 1, 0)
    carbon_data = fetch_carbon_intensity_data(from_datetime, to_datetime)
    carbon_data = refactor_intensity_column(carbon_data)
    carbon_data = add_settlement_period(carbon_data)
    carbon_data = add_date_column(carbon_data)
    carbon_data = update_column_names(carbon_data)
    print(carbon_data.head())
