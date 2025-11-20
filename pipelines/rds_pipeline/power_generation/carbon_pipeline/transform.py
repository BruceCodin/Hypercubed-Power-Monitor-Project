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

def transform_carbon_data(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Perform full transformation pipeline on carbon intensity data.

    Args:
        carbon_df (pd.DataFrame): DataFrame containing raw carbon intensity data.
    
    Returns:
        pd.DataFrame: Transformed carbon intensity data.
    '''
    carbon_df = refactor_intensity_column(carbon_df)
    carbon_df = add_settlement_period(carbon_df)
    carbon_df = add_date_column(carbon_df)
    carbon_df = update_column_names(carbon_df)
    carbon_df = make_date_datetime(carbon_df)
    logger.info("Completed full transformation of carbon intensity data")
    return carbon_df

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

def make_date_datetime(carbon_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Convert 'date' column to datetime objects.

    Args:
        carbon_df (pd.DataFrame): DataFrame containing carbon intensity data with 'date' column.

    Returns:
        pd.DataFrame: DataFrame with 'date' column as datetime objects.
    '''
    if not isinstance(carbon_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(carbon_df)}")

    if carbon_df.empty:
        return carbon_df

    if 'date' not in carbon_df.columns:
        raise ValueError("DataFrame must contain 'date' column")

    try:
        carbon_df['date'] = pd.to_datetime(carbon_df['date'])
        logger.info("Converted 'date' column to datetime objects")
        return carbon_df
    except Exception as e:
        logger.error(f"Failed to convert 'date' column: {e}")
        raise ValueError(f"Failed to convert 'date' column: {e}") from e
    
    
if __name__ == "__main__":
    # For local testing
    from_datetime = datetime(2025, 1, 1, 0, 0)
    to_datetime = datetime(2025, 1, 1, 1, 0)
    carbon_data = fetch_carbon_intensity_data(from_datetime, to_datetime)
    transformed_data = transform_carbon_data(carbon_data)
    print(transformed_data.head())
    print(transformed_data.dtypes)
