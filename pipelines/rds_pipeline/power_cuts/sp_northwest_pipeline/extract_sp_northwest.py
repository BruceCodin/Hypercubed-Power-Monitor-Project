# pylint: disable=redefined-outer-name

"""Module to extract power cut data from North West Electricity (SP Energy Networks) API.
Seems to update ~ every 5 minutes."""

from datetime import datetime
import logging
from typing import Optional, List, Dict
import requests as req

BASE_URL = "https://www.enwl.co.uk/api/power-outages/search?pageSize=1000&pageNumber=1&includeCurrent=true&includeResolved=false&includeTodaysPlanned=true&includeFuturePlanned=true&includeCancelledPlanned=false"
PROVIDER = "SP Electricity North West"

logger = logging.getLogger(__name__)


def extract_power_cut_data() -> Optional[dict]:
    """
    Fetch raw data from SP Electricity North West power cut API.

    Returns:
        dict: The raw data fetched from the SP Electricity North West power cut API.
    """

    try:
        response = req.get(BASE_URL, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        logger.info("Data extraction successful.")
        return data

    except req.exceptions.RequestException as e:
        logger.error("API request failed: %s", e)
        return None


def parse_power_cut_data(data: dict) -> list[dict]:
    """
    Parse the raw data from SP Electricity North West power cut API to extract relevant information.

    Args:
        data (dict): The raw data fetched from the SP Electricity North West power cut API.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    parsed_data = []

    if not data or 'Items' not in data:
        logger.warning("No valid data to parse.")
        return parsed_data

    for fault in data['Items']:
        parsed_data.append({
            "source_provider": PROVIDER,
            "status": fault.get("faultType"),
            "outage_date": fault.get("date"),
            "recording_time": datetime.now().isoformat(),
            "affected_postcodes": fault.get("AffectedPostcodes")
        })

    logger.info("Data parsing successful.")
    return parsed_data


def extract_data_sp_northwest() -> List[Dict]:
    """
    Main extraction function - orchestrates full extraction process.

    Returns:
        List of power cut records as list dictionaries 
        similar to JSON format
    """
    data = extract_power_cut_data()

    if data:
        parsed_data = parse_power_cut_data(data)
        return parsed_data
    
    logger.warning("Data parsing failed or no data available.")
    return None


if __name__ == "__main__":
    from pprint import pprint

    # Example usage for local testing
    pprint(extract_data_sp_northwest())