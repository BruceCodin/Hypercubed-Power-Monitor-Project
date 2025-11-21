"""Transform module for NIE Networks power cut data pipeline.
This module contains functions to transform raw JSON data extracted from
the NIE Networks API into a standardized format suitable for further processing."""

import logging
from datetime import datetime


logger = logging.getLogger(__name__)

ENTRY_COLUMNS = [
    "source_provider",
    "status",
    "outage_date",
    "recording_time",
    "affected_postcodes",
]


def transform_power_cut_data(data: list[dict]) -> list[dict]:
    """Transform function to clean raw json data and output to standard format.

    Args:
        data (list[dict]): Raw data extracted from NIE Networks API.

    Returns:
        list[dict]: Transformed data in standard format."""

    if not data:
        logger.warning("No data to transform.")
        return []

    for entry in data:

        if set(entry.keys()) != set(ENTRY_COLUMNS):
            logger.warning(
                "Data entry does not match expected columns. Skipping entry.")
            continue

        entry["affected_postcodes"] = transform_postcode(
            entry.get("affected_postcodes", ""))
        entry["status"] = transform_status(entry.get("status", ""))
        entry["outage_date"] = transform_outage_date(
            entry.get("outage_date", ""))

    logger.info("Transformed %d entries.", len(data))
    return data


def transform_postcode(postcodes: str) -> list[str]:
    """Helper function to standardize postcode format.
    By removing extra spaces and converting to uppercase.

    Args:
        postcodes (str): List of postcodes separated by semicolons.

    Returns:
        list[str]: List of standardized postcodes."""

    if not postcodes:
        logger.warning("No postcodes provided.")
        return []

    standard_postcodes = []

    postcodes_list = postcodes.split(';')
    for pc in postcodes_list:
        pc = ' '.join(pc.split())  # Remove extra spaces
        pc = pc.upper()  # Convert to uppercase
        standard_postcodes.append(pc)

    return standard_postcodes


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

    # Return unknown if status cannot be determined
    return "unknown"


def transform_outage_date(date: str) -> str:
    """Helper function to standardize outage date format.

    Args:
        date (str): Original outage date string.

    Returns:
        str: Standardized outage date string in ISO format.
    """

    # Attempt to parse date in known format and convert to ISO format
    try:
        current_year = datetime.now().year
        date_with_year = f"{date} {current_year}"
        standard_date = datetime.strptime(date_with_year, "%I:%M %p, %d %b %Y")
    except (TypeError, ValueError) as e:
        logger.warning("Error transforming outage date: %s", e)
        return ""

    return standard_date.isoformat()


def transform_nie_data(parsed_data: list[dict]) -> list[dict]:
    """Main function to transform NIE Networks power cut data.

    Args:
        parsed_data (list[dict]): Parsed data from extract_nie module.

    Returns:
        list[dict]: Transformed power cut data in standard format.
    """

    if not parsed_data:
        logger.warning("No parsed data provided for transformation.")
        return []

    transformed_data = transform_power_cut_data(parsed_data)

    return transformed_data


if __name__ == "__main__":
    # Example usage for local testing
    from pprint import pprint
    from extract_nie import extract_power_cut_data, parse_power_cut_data

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    logger.info("Starting NIE Networks power cuts transformation...")

    # Extract and parse data
    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)

    # Transform data
    standardized_data = transform_nie_data(cleaned_data)

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
