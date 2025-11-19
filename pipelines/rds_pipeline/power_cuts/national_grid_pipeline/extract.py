"""Extract power cuts data from National Grid API"""
import os
import requests
import csv
from datetime import datetime
from typing import List, Dict, Optional # For older python versions type hints 

BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action/datastore_search"
RESOURCE_ID = "292f788f-4339-455b-8cc0-153e14509d4d"
TIMEOUT = 30
# No API Key required for national grid (public dataset)
# Website says 'Update frequency: Near Real Time' but looks like it's around every 5 minutes from the website

def fetch_raw_data(limit: int = 1000) -> Optional[Dict]:
    """
    Fetch raw power cuts data from National Grid API.
    
    Args:
        limit: Maximum number of records to fetch (default: 1000 since on average there are 170 records)
        
    Returns:
        Dictionary containing API response, or None if request fails
    """
    params = {
        'resource_id': RESOURCE_ID,
        'limit': limit
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
        response.raise_for_status() # Raise error for bad responses
        return response.json()
    
    except requests.exceptions.RequestException as e: # Catch all api get request errors
        print(f"❌ API request failed: {e}")
        return None


def parse_records(raw_data: Dict) -> List[Dict]:
    """
    Extract records array from API response.
    This is because the API response has a nested structure with other irrelevant metadata.
    
    Args:
        raw_data: Raw API response dictionary
        
    Returns:
        List of record dictionaries
    """
    if not raw_data or not raw_data.get('success'):
        return []

    result = raw_data.get('result', {})
    return result.get('records', [])


def validate_record(record: Dict) -> bool:
    """
    Validate if record contains required fields.
    
    Args:
        record: Single power cut record
        
    Returns:
        True if record is valid, False otherwise
    """
    postcodes = record.get('Postcodes') 
    start_time = record.get('Start Time')

    # Check postcodes exists and is not empty
    if not postcodes or not str(postcodes).strip():
        return False

    # Check start time exists and is not empty
    if not start_time or not str(start_time).strip():
        return False

    return True


def transform_record(record: Dict) -> Dict:
    """
    Transform raw record to clean format with datetime objects.
    
    Args:
        record: Raw power cut record from API
        
    Returns:
        Cleaned dictionary with standardized field names
    """
    return {
        'postcode': record.get('Postcodes', '').strip(),
        'start_time': record.get('Start Time', ''),  # String
        'status': record.get('Status', ''),
        'data_source': 'national_grid',
        'extracted_at': datetime.now().isoformat()  # String
    }


def extract_power_cuts() -> List[Dict]:
    """
    Main extraction function - orchestrates full Extraction process.
    
    Returns:
        List of cleaned power cut records
    """
    # Fetch raw data
    raw_data = fetch_raw_data()
    if not raw_data:
        print("⚠️ No data fetched from API")
        return []

    # Parse records
    records = parse_records(raw_data)
    print(f"Fetched {len(records)} records from API")

    # Filter valid records
    valid_records = [r for r in records if validate_record(r)]
    print(f"✅ {len(valid_records)} valid records (filtered out {len(records) - len(valid_records)})")

    # Transform records
    clean_records = [transform_record(r) for r in valid_records]

    return clean_records


def save_to_csv(data: List[Dict], filename: str = 'national_grid_power_cuts.csv') -> None:
    """
    Save extracted power cuts to CSV file.
    
    Args:
        data: List of power cut dictionaries
        filename: Output CSV filename
    """
    if not data:
        print("⚠️ No data to save")
        return

    # Create data_raw directory if it doesn't exist
    output_dir = 'data_raw'
    os.makedirs(output_dir, exist_ok=True)

    # Construct full filepath
    filepath = os.path.join(output_dir, filename)

    # Define CSV columns
    fieldnames = ['postcode', 'start_time',
                  'status', 'data_source', 'extracted_at']
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)  # Direct write!

    print(f"Saved {len(data)} records to {filepath}")


if __name__ == "__main__":
    print("Starting National Grid power cuts extraction...")

    # Extract data
    power_cuts = extract_power_cuts()

    # Save to CSV
    if power_cuts:
        save_to_csv(power_cuts)
        print(f"Extraction complete! Found {len(power_cuts)} power cuts")
    else:
        print("No power cuts data extracted")
