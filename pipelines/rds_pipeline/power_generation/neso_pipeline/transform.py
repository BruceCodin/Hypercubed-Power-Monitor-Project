'''Transform module for NESO Pipeline.'''
import logging
import pandas as pd
from extract import fetch_neso_demand_data, parse_neso_demand_data
# pylint: disable = logging-fstring-interpolation

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_neso_data_columns(demand_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Update column names to match database schema
    ND -> national_demand
    TSD -> transmission_system_demand
    SETTLEMENT_DATE -> settlement_date
    SETTLEMENT_PERIOD -> settlement_period

    Args:
        demand_df (pd.DataFrame): DataFrame containing NESO demand data

    Returns:
        pd.DataFrame: Transformed DataFrame with updated column names
    '''
    if not isinstance(demand_df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(demand_df)}")

    if demand_df.empty:
        return demand_df

    try:
        column_mapping = {
            "ND": "national_demand",
            "TSD": "transmission_system_demand",
            "SETTLEMENT_DATE": "settlement_date",
            "SETTLEMENT_PERIOD": "settlement_period"
        }
        transformed_df = demand_df.rename(columns=column_mapping)
        logger.info(f"Transformed NESO data with {len(transformed_df)} rows")
        return transformed_df
    except (KeyError, ValueError) as e:
        logger.error(f"Failed to transform NESO data: {e}")
        raise

def make_date_column_datetime(demand_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Convert settlement_date column to datetime format

    Args:
        demand_df (pd.DataFrame): DataFrame containing NESO demand data

    Returns:
        pd.DataFrame: DataFrame with settlement_date as datetime
    '''
    if 'settlement_date' not in demand_df.columns:
        raise KeyError("settlement_date column not found in DataFrame")

    try:
        demand_df['settlement_date'] = pd.to_datetime(demand_df['settlement_date'])
        logger.info("Converted settlement_date to datetime format")
        return demand_df
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to convert settlement_date to datetime: {e}")
        raise

def validate_data_types(demand_df: pd.DataFrame) -> bool:
    '''
    Validate data types of NESO demand DataFrame
    national_demand int
    transmission_system_demand int
    settlement_date datetime
    settlement_period int

    Args:
        demand_df (pd.DataFrame): DataFrame containing NESO demand data
    
    Returns:
        bool: True if data types are valid, False otherwise
    '''
    # check settlement_period between 1 and 48, remove if not
    demand_df = demand_df[demand_df['settlement_period'].between(1, 48)]
    expected_types = {
        'national_demand': 'int64',
        'transmission_system_demand': 'int64',
        'settlement_date': 'datetime64[ns]',
        'settlement_period': 'int64'
    }
    for column, expected_type in expected_types.items():
        if column not in demand_df.columns:
            logger.error(f"Column {column} not found in DataFrame")
            return False
        if str(demand_df[column].dtype) != expected_type:
            logger.error(f"Column {column} has type {demand_df[column].dtype}, expected {expected_type}")
            return False
    logger.info("All data types are valid")
    return True

def transform_neso_demand_data(demand_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Full transformation pipeline for NESO demand data

    Args:
        demand_df (pd.DataFrame): Raw NESO demand data
    Returns:
        pd.DataFrame: Fully transformed NESO demand data
    '''
    
    df = transform_neso_data_columns(demand_df)
    df = make_date_column_datetime(df)
    if not validate_data_types(df):
        raise ValueError("Data validation failed")
    return df