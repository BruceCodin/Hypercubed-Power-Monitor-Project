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
        return None

    for entry in data:
        entry["affected_postcodes"] = transform_postcode(
            entry.get("affected_postcodes", []))
        entry["status"] = transform_status(entry.get("status", ""))

    return data


def transform_postcode(postcode: str) -> str:
    """Helper function to standardize postcode format.
    By removing extra spaces and converting to uppercase.

    Args:
        postcodes (list[str]): List of postcodes to standardize.

    Returns:
        list[str]: Standardized list of postcodes."""

    standard_pc = " ".join(postcode.upper().split())

    return standard_pc


def transform_status(status: str) -> str:
    """Helper function to standardize status values.

    Args:
        status (str): Original status value.

    Returns:
        str: Standardized status value.
    """
    status_mapping = {
        "Planned Work on System": "Planned",
        "Localised Fault": "Unplanned"
    }
    return status_mapping.get(status, "Unknown")


if __name__ == "__main__":

    # Example usage

    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)
    standardized_data = transform_power_cut_data(cleaned_data)

    pprint(standardized_data)
