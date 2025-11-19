"""Module to extract power cut data from SSEN API.
Seems to update ~ every 5 minutes."""

import logging
import requests as req
from typing import Optional
from datetime import datetime
from pprint import pprint

BASE_URL = "https://ssen-powertrack-api.opcld.com/gridiview/reporter/info/livefaults"
PROVIDER = "Scottish and Southern Electricity Networks"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


def extract_power_cut_data() -> Optional[dict]:
    """
    Fetch raw data from SSEN power cut API.

    Returns:
        dict: The raw data fetched from the SSEN power cut API.
    """

    try:
        response = req.get(BASE_URL)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        logger.info("Data extraction successful.")
        return data

    except req.exceptions.RequestException as e:
        logger.error("API request failed: %s", e)
        return None


def parse_power_cut_data(data: dict) -> list[dict]:
    """
    Parse the raw data from SSEN power cut API to extract relevant information.

    Args:
        data (dict): The raw data fetched from the SSEN power cut API.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    parsed_data = []

    if not data or 'Faults' not in data:
        logger.warning("No valid data to parse.")
        return parsed_data

    for fault in data['Faults']:

        parsed_entry = {
            "source_provider": PROVIDER,
            "status": fault.get("py/object"),
            "outage_date": fault.get("loggedAt"),
            "recording_time": datetime.now().isoformat(),
            "affected_postcodes": fault.get("affectedAreas"),
        }

        parsed_data.append(parsed_entry)

    logger.info("Data parsing successful.")
    return parsed_data


def get_power_cut_data() -> list[dict]:
    """
    High-level function to get parsed power cut data from SSEN API.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    raw_data = extract_power_cut_data()
    if raw_data:
        return parse_power_cut_data(raw_data)
    else:
        return []


if __name__ == "__main__":

    data = extract_power_cut_data()
    # pprint(data)

    if data:
        parsed_data = parse_power_cut_data(data)
        pprint(parsed_data)
