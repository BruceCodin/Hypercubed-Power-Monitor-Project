# pylint: disable=W1203, R0911, C0301, C0303
"""Extract power cuts data from National Grid API"""
# imports
import logging
from datetime import datetime
from typing import List, Dict, Optional
import requests
from pprint import pprint

# API configuration
BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action/datastore_search"
RESOURCE_ID = "292f788f-4339-455b-8cc0-153e14509d4d"
TIMEOUT = 30
PROVIDER = "National Grid"
# No API Key required for national grid (public dataset)
# Website says 'Update frequency: Near Real Time' but looks like it's around every 5 minutes from the website

# Logging configuration
logger = logging.getLogger(__name__)

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

        if response.status_code == 404:
            logger.error("Resource not found (404) - check RESOURCE_ID")
            return None

        if response.status_code >= 500:
            logger.error(
                f"Server error ({response.status_code}) - API temporarily unavailable")
            return None

        if response.status_code >= 400:
            logger.error(
                f"Client error ({response.status_code}) - invalid request parameters")
            return None

        # If we get here status is 2xx (success)
        return response.json()

    # Handle possible request exceptions
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout after {TIMEOUT} seconds")
        return None

    except requests.exceptions.ConnectionError:
        logger.error("Connection failed - check network or API URL")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
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
    Transform raw record to clean format matching RDS schema.
    
    Args:
        record: Raw power cut record from API
        
    Returns:
        Cleaned dictionary with standardized field names
    """
    return {
        'affected_postcodes': record.get('Postcodes', '').strip(),
        'outage_date': record.get('Start Time', ''),  # String
        'status': record.get('Planned', ''),
        'source_provider': PROVIDER,
        'recording_time': datetime.now().isoformat()  # String
    }

def extract_data_national_grid() -> List[Dict]:
    """
    Main extraction function - orchestrates full extraction process.
    
    Returns:
        List of cleaned power cut records as dictionaries
    """
    # Fetch raw data
    raw_data = fetch_raw_data()
    if not raw_data:
        logger.warning("No data fetched from API")
        return []

    # Parse records
    records = parse_records(raw_data)
    logger.info(f"Fetched {len(records)} records from API")

    # Filter valid records
    valid_records = [r for r in records if validate_record(r)]
    filtered_count = len(records) - len(valid_records)

    if filtered_count > 0:
        logger.info(f"Filtered out {filtered_count} invalid records")
    logger.info(f"Validated {len(valid_records)} records")

    # Transform records
    clean_records = [transform_record(r) for r in valid_records]

    logger.info(
        f"Data extraction successful. Returning {len(clean_records)} records")
    return clean_records


if __name__ == "__main__":
    # Example usage for local testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting National Grid power cuts extraction...")
    power_cuts = extract_data_national_grid()

    if power_cuts:
        logger.info(f"Extraction complete! Found {len(power_cuts)} power cuts")
        # Print first 10 records for inspection
        print("\n" + "="*80)
        print(f"Sample of first {min(10, len(power_cuts))} records:")
        print("="*80)
        pprint(power_cuts[:10])
    else:
        logger.warning("No power cuts data extracted")
