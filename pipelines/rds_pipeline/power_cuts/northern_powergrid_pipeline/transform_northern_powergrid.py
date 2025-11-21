""" Transform module for Northern Powergrid power cut data pipeline.
This module contains functions to transform raw JSON data extracted from
the Northern Powergrid API into a standardized format suitable for further processing."""

from datetime import datetime
import logging
from typing import Optional


logger = logging.getLogger(__name__)

ENTRY_COLUMNS = [
    "recording_time",
    "outage_date",
    "affected_postcodes",
    "status",
    "source_provider"
]


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


def transform_northern_powergrid_data(data: list[dict]) -> list[dict]:
    """Main transform function to clean raw JSON data and output to standard format.

    Args:
        data (list[dict]): Raw data extracted from Northern Powergrid API.

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
        entry["affected_postcodes"] = transform_postcode(
            entry.get("affected_postcodes", []))
        entry["status"] = transform_status(entry.get("status", ""))

    logger.info("Transformed %d power cut records.", len(data))

    return data


if __name__ == "__main__":
    # Example usage for local testing
    from pprint import pprint
    from extract_northern_powergrid import extract_power_cut_data, parse_power_cut_data

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    logger.info("Starting Northern Powergrid power cuts transformation...")

    # Extract and parse data
    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)

    # Transform data
    standardized_data = transform_northern_powergrid_data(cleaned_data)

    if standardized_data:
        logger.info(
            f"Transformation complete! Transformed {len(standardized_data)} records")
        print("\n" + "="*80)
        print(
            f"Sample of first {min(5, len(standardized_data))} transformed records:")
        print("="*80)
        pprint(standardized_data[:5])
    else:
        logger.warning("No data transformed")
