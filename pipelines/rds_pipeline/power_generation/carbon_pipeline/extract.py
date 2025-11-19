'''Extract functions for data from Carbon Intensity API.'''
import requests
from datetime import datetime

def fetch_carbon_intensity_data(from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:
    '''
    Fetch carbon intensity data from Carbon Intensity API for a specific date range
    Endpoint: https://api.carbonintensity.org.uk/intensity/{from}/{to}
    '''
    if not isinstance(from_datetime, datetime) or not isinstance(to_datetime, datetime):
        raise ValueError("from_datetime and to_datetime must be datetime objects")
    
    try:
        from_str = from_datetime.strftime("%Y-%m-%dT%H:%MZ")
        to_str = to_datetime.strftime("%Y-%m-%dT%H:%MZ")
        url = f"https://api.carbonintensity.org.uk/intensity/{from_str}/{to_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['data']
    
    except requests.RequestException as e:
        print(f"Error fetching carbon intensity data: {e}")
        return None
    
