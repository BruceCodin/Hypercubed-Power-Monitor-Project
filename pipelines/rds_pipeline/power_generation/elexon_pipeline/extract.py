'''Extract script for price and generation data from Elexon API.'''
import requests
import pandas as pd
from datetime import datetime

def fetch_elexon_price_data(date: datetime) -> pd.DataFrame:
    '''
    Fetch price data from Elexon API for a specific date
    from https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/
    '''
    if not isinstance(date, datetime):
        raise ValueError("date must be a datetime object")
    
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{date_str}"

    try: 
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"Error fetching Elexon price data: {e}")
        return None
    
def parse_elexon_price_data(raw_data: dict) -> pd.DataFrame:
    '''
    Parse raw price data from Elexon API into a DataFrame
    '''

    df = pd.DataFrame(raw_data['data'])
    # Only want settlementDate, settlementPeriod, systemSellPrice
    df = df[['settlementDate', 'settlementPeriod', 'systemSellPrice']]
    return df

def fetch_elexon_generation_data(startTime: datetime, endTime: datetime) -> pd.DataFrame:
    '''
    Fetch generation data from Elexon API for a specific date range
    https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary?startTime&endTime
    '''
    if not isinstance(startTime, datetime) or not isinstance(endTime, datetime):
        raise ValueError("startTime and endTime must be datetime objects")
    
    try:
        start_str = startTime.strftime("%Y-%m-%dT%H:%MZ")
        end_str = endTime.strftime("%Y-%m-%dT%H:%MZ")
        url = f"https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary?startTime={start_str}&endTime={end_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"Error fetching Elexon generation data: {e}")
        return None

def parse_elexon_generation_data(raw_data: list) -> pd.DataFrame:
    '''
    Parse raw generation data from Elexon API into a DataFrame
    '''

    df = pd.DataFrame(raw_data)
    return df

if __name__ == "__main__":
    pass
    # # Example usage
    # price_data_raw = fetch_elexon_price_data(datetime(2024, 1, 1))
    # price_df = parse_elexon_price_data(price_data_raw)
    # print(price_df.head())

    # generation_data_raw = fetch_elexon_generation_data(datetime(2024, 1, 1), datetime(2024, 1, 2))
    # generation_df = parse_elexon_generation_data(generation_data_raw)
    # print(generation_df.head())