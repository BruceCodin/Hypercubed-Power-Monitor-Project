import logging
import pandas as pd
import requests
# pylint: disable = logging-fstring-interpolation

logger = logging.getLogger(__name__)

URL = "https://api.neso.energy/api/3/action/datastore_search_sql"
TIME_OUT = 10

def fetch_neso_demand_data(resource_id: str) -> dict:
    """
    Fetch demand data from NESO API using SQL query

    Args:
        resource_id (str): The resource ID for the NESO dataset.
    Returns:
        pd.DataFrame: DataFrame containing the NESO demand data.
    """
    logger.info(f"Fetching NESO demand data for resource_id: {resource_id}")

    sql_query = f"""
    SELECT *
    FROM  "{resource_id}"
    ORDER BY "_id"
    """
    params = {"sql": sql_query}
    try:
        response = requests.get(URL, params=params, timeout=TIME_OUT)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data["result"]["records"])
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

