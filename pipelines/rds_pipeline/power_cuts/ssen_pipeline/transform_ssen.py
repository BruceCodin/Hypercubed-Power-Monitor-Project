""" Transform module for SSEN power cut data pipeline.
This module contains functions to transform raw JSON data extracted from
the SSEN API into a standardized format suitable for further processing."""

from datetime import datetime
import logging


logger = logging.getLogger(__name__)

ENTRY_COLUMNS = [
    "recording_time",
    "outage_date",
    "affected_postcodes",
    "status",
    "source_provider"
]


def transform_ssen_data(data: list[dict]) -> list[dict]:
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

        # Transform status field
        entry["status"] = transform_status(entry.get("status", ""))

    logger.info("Transformed %d power cut records.", len(data))

    return data


def transform_status(status: str) -> str:
    """Helper function to standardize status values.
    PSI --> Planned Supply Interruption
    Any other value is considered unplanned.

    Args:
        status (str): Original status value.

    Returns:
        str: Standardized status value.
    """

    if status == "PSI":
        return "planned"
    return "unplanned"


if __name__ == "__main__":
    # Example usage for local testing
    from pprint import pprint
    from extract_ssen import extract_power_cut_data, parse_power_cut_data

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    logger.info("Starting SSEN power cuts transformation...")

    # Extract and parse data
    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)

    # Transform data
    standardized_data = transform_ssen_data(cleaned_data)

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
