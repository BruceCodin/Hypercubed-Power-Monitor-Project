'''Module to extract NESO demand data via API calls'''
import logging
import pandas as pd
import requests
# pylint: disable = logging-fstring-interpolation

logger = logging.getLogger(__name__)

URL = "https://api.neso.energy/api/3/action/datastore_search_sql"
TIME_OUT = 10
RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"

def fetch_neso_demand_data(settlement_date: str, settlement_period: int) -> pd.DataFrame:
    """
    Fetch demand data from NESO API using SQL query
    
    Filters for records after specified settlement date/period and excludes forecasts.

    Args:
        settlement_date (str): The settlement date to filter from (format: 'YYYY-MM-DD').
        settlement_period (int): The settlement period to filter from.
    Returns:
        pd.DataFrame: DataFrame containing the NESO demand data, or None if error occurs.
    """
    logger.info(f"Fetching NESO demand data after {settlement_date} period {settlement_period}")

    sql_query = f"""
    SELECT *
    FROM  "{RESOURCE_ID}"
    WHERE (
        "SETTLEMENT_DATE" > '{settlement_date}'
        OR ("SETTLEMENT_DATE" = '{settlement_date}' AND "SETTLEMENT_PERIOD" > {settlement_period})
    )
    AND "FORECAST_ACTUAL_INDICATOR" != 'F'
    ORDER BY "_id" desc
    """
    params = {"sql": sql_query}
    try:
        response = requests.get(URL, params=params, timeout=TIME_OUT)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data["result"]["records"])
        if df.empty:
            logger.info("No new NESO demand data found")
            raise ValueError("No new data found")
        logger.info(f"Successfully fetched {len(df)} records from NESO API")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching NESO demand data: {e}", exc_info=True)
        return None

def parse_neso_demand_data(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Get columns ND, TSD, SETTLEMENT_DATE, SETTLEMENT_PERIOD from NESO demand data
    Args:
        data (pd.DataFrame): DataFrame containing the NESO demand data.
    Returns:
        pd.DataFrame: DataFrame with selected columns.
    '''
    logger.info("Parsing NESO demand data")

    required_columns = ["ND", "TSD", "SETTLEMENT_DATE", "SETTLEMENT_PERIOD"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    result_df = data[required_columns]
    logger.info(f"Parsed {len(result_df)} records with required columns")
    return result_df


