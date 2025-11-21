# pylint: disable=W1203, R0911, C0301, C0303
"""Extract power cuts data from UK Power Networks API"""
# imports
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional
import requests

# API configuration
BASE_URL = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1"
DATASET_ID = "ukpn-live-faults"
API_ENDPOINT = f"{BASE_URL}/catalog/datasets/{DATASET_ID}/records"
TIMEOUT = 30
PROVIDER = "UK Power Networks"

# Logging configuration
logger = logging.getLogger(__name__)


def fetch_raw_data(limit: int = 100) -> Optional[Dict]:
    """
    Fetch raw power cuts data from UK Power Networks API.
    
    Args:
        limit: Maximum number of records to fetch (default: 100, API max is 100)
    
    Returns:
        Dictionary containing API response, or None if request fails
    """
    # Get API key from environment variable
    api_key = os.getenv('UKPN_API_KEY')

    # Try with API key if provided, otherwise try without
    headers = {}
    if api_key:
        headers = {"Authorization": f"Apikey {api_key}"}

    # API parameters
    params = {
        "limit": limit,
        "timezone": "Europe/London"
    }

    try:
        response = requests.get(
            API_ENDPOINT, headers=headers, params=params, timeout=TIMEOUT)

        if response.status_code == 401:
            logger.error("Unauthorized (401) - check API key")
            return None

        if response.status_code == 404:
            logger.error("Resource not found (404) - check API URL")
            return None

        if response.status_code >= 500:
            logger.error(
                f"Server error ({response.status_code}) - API temporarily unavailable")
            return None

        if response.status_code >= 400:
            logger.error(
                f"Client error ({response.status_code}) - invalid request")
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


def parse_records(raw_data: Optional[Dict]) -> List[Dict]:
    """
    Extract records array from API response.
    Opendatasoft API nests data under 'results' key.
    
    Args:
        raw_data: Raw API response dictionary
        
    Returns:
        List of record dictionaries
    """
    if not raw_data:
        return []

    # Opendatasoft format: data is under 'results' key
    results = raw_data.get('results', [])

    # Extract the actual fields from each result
    records = []
    for result in results:
        # Opendatasoft nests fields under 'fields' or directly in result
        if 'fields' in result:
            records.append(result['fields'])
        elif 'record' in result and 'fields' in result['record']:
            records.append(result['record']['fields'])
        else:
            records.append(result)

    return records


def validate_record(record: Dict) -> bool:
    """
    Validate if record contains required fields.
    
    Args:
        record: Single power cut record
        
    Returns:
        True if record is valid, False otherwise
    """
    # Check postcodesaffected exists and is not empty
    postcodes = record.get('postcodesaffected')
    if not postcodes or not str(postcodes).strip():
        return False

    # Check creationdatetime exists and is not empty
    creation_date = record.get('creationdatetime')
    if not creation_date or not str(creation_date).strip():
        return False

    return True


def transform_record(record: Dict) -> Dict:
    """
    Transform raw record to clean format matching RDS schema.
    
    Args:
        record: Raw power cut record from API
        
    Returns:
        Cleaned dictionary with standardized field names for RDS
    """
    return {
        'source_provider': PROVIDER,
        'status': record.get('powercuttype', '').strip(),
        'outage_date': record.get('creationdatetime', ''),
        'recording_time': datetime.now().isoformat(),
        'affected_postcodes': record.get('postcodesaffected', '').strip()
    }


def extract_data_uk_pow() -> List[Dict]:
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
    from pprint import pprint
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    # Example usage for local testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting UK Power Networks power cuts extraction...")
    power_cuts = extract_data_uk_pow()

    if power_cuts:
        logger.info(f"Extraction complete! Found {len(power_cuts)} power cuts")
        # Print first 10 records for inspection
        print(f"Sample of first {min(10, len(power_cuts))} records:")
        print("="*80)
        pprint(power_cuts[:10])
    else:
        logger.warning("No power cuts data extracted")
