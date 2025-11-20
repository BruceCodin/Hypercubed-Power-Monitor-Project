import logging
from datetime import datetime
from pprint import pprint
from extract import (extract_power_cut_data,
                     parse_power_cut_data)


logger = logging.getLogger(__name__)


def transform_power_cut_data(data: list[dict]) -> list[dict]:
    """Transform function to clean raw json data and output to standard format.

    Args:
        data (list[dict]): Raw data extracted from Northern Powergrid API.

    Returns:
        list[dict]: Transformed data in standard format."""

    if not data:
        logger.warning("No data to transform.")
        return None

    for entry in data:
        entry["affected_postcodes"] = transform_postcode(
            entry.get("affected_postcodes", ""))
        entry["status"] = transform_status(entry.get("status", ""))
        entry["outage_date"] = transform_outage_date(
            entry.get("outage_date", ""))

    logger.info("Data transformation complete.")
    return data


def transform_postcode(postcodes: str) -> list[str]:
    """Helper function to standardize postcode format.
    By removing extra spaces and converting to uppercase.

    Args:
        postcode (str): Single postcode to standardize.

    Returns:
        list[str]: Single postcode as a list."""

    if not postcodes:
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
    elif "planned" in status.lower():
        return "planned"
    else:
        return "unknown"


def transform_outage_date(date: str) -> str:
    """Helper function to standardize outage date format.

    Args:
        date (str): Original outage date string.

    Returns:
        str: Standardized outage date string in ISO format.
    """

    standard_date = datetime.strptime(date, "%I:%M %p, %d %b")
    standard_date = standard_date.replace(
        year=datetime.now().year)  # Add current year

    return standard_date.isoformat()


if __name__ == "__main__":

    # Example usage

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    raw_data = extract_power_cut_data()
    logger.info("Raw data extracted.")

    cleaned_data = parse_power_cut_data(raw_data)
    logger.info("Raw data parsed.")

    standardized_data = transform_power_cut_data(cleaned_data)
    logger.info("Data transformed to standard format.")

    pprint(standardized_data)
