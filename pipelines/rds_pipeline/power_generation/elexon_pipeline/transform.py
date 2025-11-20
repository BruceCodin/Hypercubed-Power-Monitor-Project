'''Transform module for Elexon Pipeline.'''
import logging
import ast
import pandas as pd
from extract import fetch_elexon_price_data, parse_elexon_price_data, fetch_elexon_generation_data
import datetime
# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        #rename fuelType to fuel_type
        df_expanded = df_expanded.rename(columns={'fuelType': 'fuel_type'})
        logger.info(f"Expanded generation data from {len(generation_df)} to {len(df_expanded)} rows")
        return df_expanded
    except Exception as e:
        logger.error(f"Failed to expand generation data column: {e}")
        raise

def add_date_column_to_generation(genearation_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Add settlement_date column to generation DataFrame from 'startTime' column.
    Drop startTime column.

    Args:
        genearation_df (pd.DataFrame): DataFrame containing generation data.

    Returns:
        pd.DataFrame: DataFrame with added settlement_date column.
    '''
    if not isinstance(genearation_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(genearation_df)}")

    if genearation_df.empty:
        return genearation_df

    if 'startTime' not in genearation_df.columns:
        raise ValueError("DataFrame must contain 'startTime' column")

    try:
        genearation_df['settlement_date'] = pd.to_datetime(genearation_df['startTime']).dt.date
        genearation_df = genearation_df.drop(columns=['startTime'])
        #rename settlementPeriod to settlement_period
        genearation_df = genearation_df.rename(columns={'settlementPeriod': 'settlement_period'})
        logger.info(f"Added settlement_date column to {len(genearation_df)} rows")
        return genearation_df
    except Exception as e:
        logger.error(f"Failed to add date column: {e}")
        raise


if __name__ == '__main__':
    pass
    # # Fetch and transform price data
    # fetch_date = datetime.datetime(2023, 1, 1)
    # raw_price_data = fetch_elexon_price_data(fetch_date)
    # price_df = parse_elexon_price_data(raw_price_data)
    # updated_price_df = update_price_column_names(price_df)
    # print(updated_price_df.head())

    # # Fetch and transform generation data
    # start_time = datetime.datetime(2023, 1, 1, 0, 0)
    # end_time = datetime.datetime(2023, 1, 1, 1, 0)
    # generation_df = fetch_elexon_generation_data(start_time, end_time)
    # expanded_generation_df = expand_generation_data_column(generation_df)
    # final_generation_df = add_date_column_to_generation(expanded_generation_df)
    # print(final_generation_df.head())