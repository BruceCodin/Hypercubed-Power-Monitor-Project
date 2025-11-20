import logging
from datetime import datetime
from extract import (extract_power_cut_data,
                     parse_power_cut_data)


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


if __name__ == "__main__":

    # Example usage for local testing

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)
    standardized_data = transform_power_cut_data(cleaned_data)
