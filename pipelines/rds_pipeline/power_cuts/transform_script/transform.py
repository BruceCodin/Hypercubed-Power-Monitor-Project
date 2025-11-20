'''
Clean and validate power cut data for RDS pipeline.

Inputs: JSON power cut data with columns: 
{
    "affected_postcodes": list[str],
    "outage_date": datetime,
    "source_provider": str,
    "status": str(planned, unplanned, ...) | bool,
    "recording_time": datetime
}

Outputs: clean JSON data with columns:
{
    "affected_postcodes": list[str(postcode unit)],
    "outage_date": datetime(iso format),
    "source_provider": str,
    "status": str(planned, unplanned),
    "recording_time": datetime(iso format)
}
'''

import requests
import logging
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_postcode_with_api(postcode: str) -> str | None:
    '''
    Validate and transform postcode unit using postcodes.io API.
    This matches postcode unit inputs such as 
    "BR8 7RE", "br87re", "Br8 7rE", etc.
    and returns the standardized format "BR8 7RE".

    Args:
        postcode (str): The postcode to validate.

    Returns:
        str | None: The standardized postcode if valid, None otherwise.
    '''
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    logger.info(f"Validating postcode: {postcode}")
    response = requests.get(url)

    if response.status_code == 404:
        logger.warning(f"Postcode {postcode} is invalid.")
        return None

    if response.status_code == 200:
        data = response.json()
        logger.info(f"Postcode {postcode} is valid.")
        postcode = data['result']['postcode']
        return postcode

    logger.error(
        f"Error validating postcode {postcode}: {response.status_code}.")
    postcode = transform_postcode_manually(postcode)
    if postcode is None:
        logger.warning(f"Postcode {postcode} is invalid (manual check).")
        return None
    logger.info(f"Postcode {postcode} is valid (manual check).")
    return postcode


def transform_postcode_manually(postcode: str) -> str | None:
    '''
    Helper fn: transform postcode unit manually if API unavailable.
    Validates with regex (simplest approach to cover most cases).
    UK postcode format: outward (2-4 chars) + inward (3 chars).
       Outward: 1-2 letters + digit [+ letter] OR digit + letter
        Inward: digit + 2 letters
    Format: https://ideal-postcodes.co.uk/guides/uk-postcode-format

    Args:
        postcode (str): Postcode to validate.

    Returns:
        str | None: Validated and transformed postcode, or None if invalid.
    '''
    if not isinstance(postcode, str):
        return None

    postcode = postcode.strip().upper()

    pattern = r'^([A-Z]{1,2}[0-9][A-Z0-9]?|[A-Z][0-9]{1,2})\s?([0-9][A-Z]{2})$'

    match = re.match(pattern, postcode)
    if not match:
        return None

    outward, inward = match.groups()
    return f'{outward} {inward}'


# outage_date.isoformat()
# Not required as a function


def transform_source_provider(source_provider: str) -> str | None:
    '''
    Standardize source provider string to title case.

    Args:
        source_provider (str): The source provider name.

    Returns:
        str: Standardized source provider name or None if invalid.
    '''
    if not isinstance(source_provider, str):
        return None

    return source_provider.title()
