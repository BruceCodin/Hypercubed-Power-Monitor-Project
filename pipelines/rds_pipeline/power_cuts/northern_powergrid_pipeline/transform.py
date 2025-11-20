""" Transform module for Northern Powergrid power cut data pipeline.
This module contains functions to transform raw JSON data extracted from
the Northern Powergrid API into a standardized format suitable for further processing."""

import logging
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
    elif "planned" in status.lower():
        return "planned"
    else:
        return "unknown"


if __name__ == "__main__":

    # Example usage

    raw_data = extract_power_cut_data()
    cleaned_data = parse_power_cut_data(raw_data)
    standardized_data = transform_power_cut_data(cleaned_data)

    pprint(standardized_data)
