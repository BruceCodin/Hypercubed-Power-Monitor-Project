# pylint: disable=redefined-outer-name

"""Module to extract power cut data from Northern Powergrid API.
Seems to update every ~30 minutes."""

from datetime import datetime
import logging
from typing import Optional
import requests as req

BASE_URL = "https://power.northernpowergrid.com/Powercut_API/rest/powercuts/getall"
PROVIDER = "Northern Powergrid"

logger = logging.getLogger(__name__)


def extract_power_cut_data() -> Optional[dict]:
    """
    Fetch raw data from Northern Powergrid power cut API.

    Returns:
        dict: The raw data fetched from the Northern Powergrid power cut API.
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


def parse_power_cut_data(data: dict) -> Optional[list[dict]]:
    """
    Parse the raw data from Northern Powergrid power cut API to extract relevant information.

    Args:
        data (dict): The raw data fetched from the Northern Powergrid power cut API.

    Returns:
        list[dict]: A list of dictionaries containing parsed power cut information.
    """

    parsed_data = []

    if not data:
        logger.warning("No valid data to parse.")
        return parsed_data

    for fault in data:
        parsed_data.append({
            "source_provider": PROVIDER,
            "status": fault.get("NatureOfOutage"),
            "outage_date": fault.get("LoggedTime"),
            "recording_time": datetime.now().isoformat(),
            "affected_postcodes": fault.get("Postcode")
        })

    logger.info("Data parsing successful.")
    return parsed_data


def extract_northern_powergrid_data() -> Optional[list[dict]]:
    """
    Main extraction function - orchestrates full extraction process.

    Returns:
        list[dict]: List of cleaned power cut records as dictionaries
    """

    # Fetch raw data
    raw_data = extract_power_cut_data()
    if not raw_data:
        logger.warning("No data fetched from API")
        return []

    # Parse records
    parsed_data = parse_power_cut_data(raw_data)
    logger.info(f"Fetched {len(parsed_data)} records from API")

    return parsed_data


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Example usage for local testing

    data = extract_power_cut_data()

    if data:
        parsed_data = parse_power_cut_data(data)
        print("Extracted and Parsed Data:")
        for entry in parsed_data:
            print(entry)
            print("-----")
        print(f"Total records extracted: {len(parsed_data)}")
