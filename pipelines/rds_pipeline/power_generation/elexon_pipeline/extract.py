'''Extract script for price and generation data from Elexon API.'''
import requests
import pandas as pd
from datetime import datetime

def fetch_elexon_price_data(date: datetime) -> pd.DataFrame:
    '''
    Fetch price data from Elexon API for a specific date
    from https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/
    '''
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{date_str}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data

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
    start_str = startTime.strftime("%Y-%m-%dT%H:%MZ")
    end_str = endTime.strftime("%Y-%m-%dT%H:%MZ")
    url = f"https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary?startTime={start_str}&endTime={end_str}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data

def parse_elexon_generation_data(raw_data: dict) -> pd.DataFrame:
    '''
    Parse raw generation data from Elexon API into a DataFrame
    '''

    df = pd.DataFrame(raw_data[0])
    return df

if __name__ == "__main__":
    # # Example usage
    # price_data_raw = fetch_elexon_price_data(datetime(2024, 1, 1))
    # price_df = parse_elexon_price_data(price_data_raw)
    # print(price_df.head())

    # generation_data_raw = fetch_elexon_generation_data(datetime(2024, 1, 1), datetime(2024, 1, 2))
    # generation_df = parse_elexon_generation_data(generation_data_raw)
    # print(generation_df.head())