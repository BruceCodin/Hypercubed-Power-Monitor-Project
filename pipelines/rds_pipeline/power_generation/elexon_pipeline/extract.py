'''Extract script for price and generation data from Elexon API.'''
import logging
import requests
import pandas as pd
from datetime import datetime
# pylint: disable = logging-fstring-interpolation

logger = logging.getLogger(__name__)

PRICE_URL = "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/"
GENERATION_URL = "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary"
TIME_OUT = 30

def fetch_elexon_price_data(fetch_date: datetime) -> pd.DataFrame:
    '''
    Fetch price data from Elexon API for a specific date
     Args:
        date (datetime): Date for which to fetch price data
    Returns:
        dict: Raw JSON data from Elexon API
    '''
    if not isinstance(fetch_date, datetime):
        raise ValueError("date must be a datetime object")
    
    date_str = fetch_date.strftime("%Y-%m-%d")
    url = f"{PRICE_URL}{date_str}"

    logger.info(f"Fetching Elexon price data for date: {date_str}")

    try:
        response = requests.get(url, timeout=TIME_OUT)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched Elexon price data for {date_str}")
        return data

    except requests.RequestException as e:
        logger.error(f"Error fetching Elexon price data: {e}", exc_info=True)
        return None
    
def parse_elexon_price_data(raw_data: dict) -> pd.DataFrame:
    '''
    Parse raw price data from Elexon API into a DataFrame

    Args:
        raw_data (dict): Raw JSON data from Elexon API
    Returns:
        pd.DataFrame: Parsed price data
    '''
    logger.info("Parsing Elexon price data")

    df = pd.DataFrame(raw_data['data'])
    # Only want settlementDate, settlementPeriod, systemSellPrice
    df = df[['settlementDate', 'settlementPeriod', 'systemSellPrice']]
    logger.info(f"Parsed {len(df)} price records")
    return df

def fetch_elexon_generation_data(startTime: datetime, endTime: datetime) -> pd.DataFrame:
    '''
    Fetch generation data from Elexon API for a specific date range
    
    Args:
        startTime (datetime): Start time for data fetch
        endTime (datetime): End time for data fetch
        
    Returns:
        pd.DataFrame: Parsed generation data
    '''
    if not isinstance(startTime, datetime) or not isinstance(endTime, datetime):
        raise ValueError("startTime and endTime must be datetime objects")

    start_str = startTime.strftime("%Y-%m-%dT%H:%MZ")
    end_str = endTime.strftime("%Y-%m-%dT%H:%MZ")

    logger.info(f"Fetching Elexon generation data from {start_str} to {end_str}")

    try:
        url = f"{GENERATION_URL}?startTime={start_str}&endTime={end_str}"
        response = requests.get(url, timeout=TIME_OUT)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logger.info(f"Successfully fetched {len(df)} generation records from Elexon API")
        return df
    except requests.RequestException as e:
        logger.error(f"Error fetching Elexon generation data: {e}", exc_info=True)
        return None
