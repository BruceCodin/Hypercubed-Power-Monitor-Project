'''Extract functions for data from Carbon Intensity API.'''
import logging
from datetime import datetime
import requests
import pandas as pd
# pylint: disable = logging-fstring-interpolation

logger = logging.getLogger(__name__)

URL = "https://api.carbonintensity.org.uk/intensity/"
TIME_OUT = 30

def fetch_carbon_intensity_data(from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:
    '''
    Fetch carbon intensity data from Carbon Intensity API for a specific date range

    Args: 
        from_datetime (datetime): Start datetime for data extraction
        to_datetime (datetime): End datetime for data extraction
    Returns:
        pd.DataFrame: DataFrame containing carbon intensity data
    '''
    if not isinstance(from_datetime, datetime) or not isinstance(to_datetime, datetime):
        raise ValueError("from_datetime and to_datetime must be datetime objects")

    from_str = from_datetime.strftime("%Y-%m-%dT%H:%MZ")
    to_str = to_datetime.strftime("%Y-%m-%dT%H:%MZ")

    logger.info(f"Fetching carbon intensity data from {from_str} to {to_str}")

    try:
        url = f"{URL}{from_str}/{to_str}"
        response = requests.get(url, timeout = TIME_OUT)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['data'])
        logger.info(f"Successfully fetched {len(df)} records from Carbon Intensity API")
        return df

    except requests.RequestException as e:
        logger.error(f"Error fetching carbon intensity data: {e}", exc_info=True)
        return None
    