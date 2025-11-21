# pylint: disable=redefined-outer-name

"""Module to extract power cut data from NIE Networks API.
Seems to update ~ every 5 minutes."""

from datetime import datetime
import logging
from typing import Optional
import requests as req

BASE_URL = "https://powercheck.nienetworks.co.uk/NIEPowerCheckerWebAPI/api/faults"
PROVIDER = "Northern Ireland Electricity Networks"
API_TIMEOUT = 10  # seconds

logger = logging.getLogger(__name__)


def extract_power_cut_data() -> Optional[dict]:
    """
    Fetch raw data from NIE Networks power cut API.

    Returns:
        dict: The raw data fetched from the NIE Networks power cut API.
    """

    try:
        response = req.get(BASE_URL, timeout=API_TIMEOUT)

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch data: Status code {response.status_code}")
            return None

        data = response.json()
        logger.info("Data extraction successful.")
        return data

    except req.exceptions.RequestException as e:
        logger.error("API request failed: %s", e)
        return None


def parse_power_cut_data(data: Optional[dict]) -> Optional[list[dict]]:
    """
    Parse the raw data from NIE Networks power cut API to extract relevant information.

    Args:
        data (dict): The raw data fetched from the NIE Networks power cut API.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    parsed_data = []

    if not data or 'outageMessage' not in data:
        logger.warning("No valid data to parse.")
        return parsed_data

    for fault in data['outageMessage']:
        parsed_entry = {
            "source_provider": PROVIDER,
            "status": fault.get("outageType"),
            "outage_date": fault.get("startTime"),
            "recording_time": datetime.now().isoformat(),
            "affected_postcodes": fault.get("fullPostCodes"),
        }
        parsed_data.append(parsed_entry)

    logger.info("Data parsing successful.")
    return parsed_data


def extract_nie_data() -> Optional[list[dict]]:
    """
    Main function to extract and parse NIE Networks power cut data.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    raw_data = extract_power_cut_data()
    if raw_data is None:
        return None

    parsed_data = parse_power_cut_data(raw_data)
    return parsed_data


if __name__ == "__main__":

    from pprint import pprint
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Example usage for local testing

    data = extract_power_cut_data()

    if data:
        parsed_data = parse_power_cut_data(data)
        print("Extracted and Parsed Data:")
        pprint(parsed_data)
