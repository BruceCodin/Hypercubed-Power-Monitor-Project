'''Transform module for Elexon Pipeline.'''
import logging
import ast
import pandas as pd
from datetime import datetime
from extract_elexon import fetch_elexon_generation_data
# Configure logger
logger = logging.getLogger(__name__)

def update_price_column_names(price_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Update column names to standard format
    Args:
        price_df (pd.DataFrame): DataFrame with original column names
    Returns:
        pd.DataFrame: DataFrame with updated column names
    '''
    if not isinstance(price_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(price_df)}")

    if price_df.empty:
        return price_df

    try:
        price_df = price_df.rename(columns={
            'settlementDate': 'date',
            'settlementPeriod': 'settlement_period',
            'systemSellPrice': 'system_sell_price'
        })
        # Convert date column to datetime type
        price_df['date'] = pd.to_datetime(price_df['date'])
        logger.info("Updated price column names")
        return price_df
    except Exception as e:
        logger.error(f"Failed to update price column names: {e}")
        raise

def expand_generation_data_column(generation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand data column in the generation DataFrame.

    Args:
        generation_df (pd.DataFrame): DataFrame containing the generation data.

    Returns:
        pd.DataFrame: Expanded DataFrame with individual generation records.
    """
    if not isinstance(generation_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(generation_df)}")

    if generation_df.empty:
        return generation_df

    if 'data' not in generation_df.columns:
        raise ValueError("DataFrame must contain 'data' column")

    try:
        # Parse the data if it's a string, otherwise leave as is
        def parse_if_string(x):
            if isinstance(x, str):
                return ast.literal_eval(x)
            return x

        generation_df['data'] = generation_df['data'].apply(parse_if_string)

        # Explode and normalize in one go
        df_expanded = generation_df.explode('data').reset_index(drop=True)
        df_expanded = pd.concat([df_expanded.drop('data', axis=1),
                                df_expanded['data'].apply(pd.Series)], axis=1)
        #Rename fuelType to fuel_type
        df_expanded = df_expanded.rename(columns={'fuelType': 'fuel_type'})
        logger.info(f"Expanded generation data from {len(generation_df)} to {len(df_expanded)} rows")
        return df_expanded
    except Exception as e:
        logger.error(f"Failed to expand generation data column: {e}")
        raise

def add_date_column_to_generation(generation_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Add settlement_date column to generation DataFrame from 'startTime' column.
    Drop startTime column.

    Args:
        generation_df (pd.DataFrame): DataFrame containing generation data.

    Returns:
        pd.DataFrame: DataFrame with added settlement_date column.
    '''
    if not isinstance(generation_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(generation_df)}")

    if generation_df.empty:
        return generation_df
    if 'startTime' not in generation_df.columns:
        raise ValueError("DataFrame must contain 'startTime' column")

    try:
        generation_df['date'] = pd.to_datetime(generation_df['startTime']).dt.date
        # Make date datetime type
        generation_df['date'] = pd.to_datetime(generation_df['date'])
        #Drop startTime column
        generation_df = generation_df.drop(columns=['startTime'])
        #Rename settlementPeriod to settlement_period
        generation_df = generation_df.rename(columns={'settlementPeriod': 'settlement_period'})
        logger.info(f"Added settlement_date column to {len(generation_df)} rows")
        return generation_df
    except Exception as e:
        logger.error(f"Failed to add date column: {e}")
        raise


def aggregate_generation_by_settlement_period(generation_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aggregate generation data by settlement period using the sum of all 5-minute readings.
    This combines the 6 five-minute intervals within each settlement period into a single value.

    Args:
        generation_df (pd.DataFrame): DataFrame containing generation data with date, settlement_period, and fuel_type.
    Returns:
        pd.DataFrame: Aggregated generation data with one row per (date, settlement_period, fuel_type).
    '''
    if generation_df.empty:
        return generation_df

    try:
        # Group by date, settlement_period, and fuel_type, then sum the generation values
        aggregated_df = generation_df.groupby(
            ['date', 'settlement_period', 'fuel_type'],
            as_index=False
        )['generation'].sum()

        logger.info(f"Aggregated generation data from {len(generation_df)} to {len(aggregated_df)} rows")
        return aggregated_df
    except Exception as e:
        logger.error(f"Failed to aggregate generation data: {e}")
        raise


def transform_generation_data(generation_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform generation data by expanding data column, adding settlement_date column, and aggregating by settlement period.

    Args:
        generation_df (pd.DataFrame): DataFrame containing generation data.
    Returns:
        pd.DataFrame: Transformed and aggregated generation data.
    '''
    expanded_df = expand_generation_data_column(generation_df)
    transformed_df = add_date_column_to_generation(expanded_df)
    aggregated_df = aggregate_generation_by_settlement_period(transformed_df)
    return aggregated_df

# if __name__ == "__main__":
#     # Example usage
#     generation_df = fetch_elexon_generation_data(startTime = datetime(2025,11,24,0,0), endTime = datetime(2025,11,25,0,0))
#     transformed_data = transform_generation_data(generation_df)
#     print(transformed_data.head())
#     #count unique fuel type period combinations
#     unique_combinations = transformed_data[['date', 'settlement_period', 'fuel_type']].drop_duplicates()
#     print(f"Unique (date, settlement_period, fuel_type) combinations: {len(unique_combinations)}")
