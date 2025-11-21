""" Transform module for Northern Powergrid power cut data pipeline.
This module contains functions to transform raw JSON data extracted from
the SSEN API into a standardized format suitable for further processing."""

from datetime import datetime
import logging
from typing import Optional
from pprint import pprint
from extract_ssen import (extract_power_cut_data,
                          parse_power_cut_data)


logger = logging.getLogger(__name__)

ENTRY_COLUMNS = [
    "recording_time",
    "outage_date",
    "affected_postcodes",
    "status",
    "source_provider"
]


def transform_power_cut_data(data: list[dict]) -> list[dict]:
    """Transform function to clean raw JSON data and output to standard format.

    Args:
        data (list[dict]): Raw data extracted from SSEN API.

    Returns:
        list[dict]: Transformed data in standard format."""

    if not data:
        logger.warning("No data to transform.")
        return None

    for entry in data:

        # Validate presence of expected keys
        if any(key not in entry for key in ENTRY_COLUMNS):
            logger.warning("Missing expected keys in entry: %s", entry)
            continue

        # Validate and transform date format
        try:
            datetime.fromisoformat(entry.get("outage_date"))
        except (ValueError, TypeError):
            logger.info("Invalid date format for entry: %s", entry)

        # Transform affected postcodes and status

        pprint(entry)

    logger.info("Transformed %d power cut records.", len(data))

    return data


def transform_postcode(postcode: str) -> list[str]:
    """Helper function to standardize postcode format.
    By removing extra spaces and converting to uppercase.

    Args:
        postcode (str): Single postcode to standardize.

    Returns:
        list[str]: Single postcode as a list."""

    if not postcode:
        return []

    standard_pc = " ".join(postcode.upper().split())

    # Return as a list to maintain consistency with expected data structure
    return [standard_pc]


def transform_status(status: str) -> str:
    """Helper function to standardize status values.

    Args:
        status (str): Original status value.

    Returns:
        str: Standardized status value.
    """

    if "fault" in status.lower():
        return "unplanned"
    if "planned" in status.lower():
        return "planned"

    # If status is unrecognized, return 'unknown'
    return "unknown"


def transform_ssen_data() -> Optional[list[dict]]:
    """
    Main transformation function - orchestrates full transformation process.

    Returns:
        list[dict]: List of cleaned power cut records as dictionaries
    """

    # Extract raw data
    raw_data = extract_power_cut_data()
    if not raw_data:
        logger.warning("No data extracted from API")
        return []

    # Parse records
    parsed_data = parse_power_cut_data(raw_data)

    # Transform records
    transformed_data = transform_power_cut_data(parsed_data)

    return transformed_data


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    # Example usage

    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)
    standardized_data = transform_power_cut_data(cleaned_data)
