'''
Clean and validate power cut data for RDS pipeline.

Inputs: JSON power cut data with columns: 
{
    "affected_postcodes": list[str],
    "outage_date": datetime,
    "source_provider": str,
    "status": str | bool,
    "recording_time": datetime
}

Outputs: clean JSON data with columns:
{
    "affected_postcodes": list[str(postcode unit)],
    "outage_date": datetime(iso format),
    "source_provider": str,
    "status": str(default planned, unplanned),
    "recording_time": datetime(iso format)
}
'''

import logging
import re

import requests

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
    logger.info("Validating postcode: %s", postcode)
    response = requests.get(url, timeout=5)

    if response.status_code == 404:
        logger.warning("Postcode %s is invalid.", postcode)
        return None

    if response.status_code == 200:
        data = response.json()
        logger.info("Postcode %s is valid.", postcode)
        postcode = data['result']['postcode']
        return postcode

    logger.error(
        "Error validating postcode %s: %s.", postcode, response.status_code)
    postcode = transform_postcode_manually(postcode)
    if postcode is None:
        logger.warning("Postcode %s is invalid (manual check).", postcode)
        return None
    logger.info("Postcode %s is valid (manual check).", postcode)
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


def transform_postcode_list(postcode_list: list[str]) -> list[str]:
    '''
    Transform a list of postcode units, validating each.

    Args:
        postcode_list (list[str]): List of postcode strings.

    Returns:
        list[str]: List of validated and transformed postcode strings.
    '''
    transformed_list = []
    for postcode in postcode_list:
        transformed_postcode = transform_postcode_with_api(postcode)
        if transformed_postcode:
            transformed_list.append(transformed_postcode)
    return transformed_list


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

    source_provider_words = source_provider.split(" ")
    for i, word in enumerate(source_provider_words):
        source_provider_words[i] = word.strip().title()
    source_provider = ' '.join(source_provider_words)

    if not source_provider:
        return None

    return source_provider


def transform_status(status: str | bool) -> str | None:
    '''
    Standardize boolean status to "planned" or "unplanned".
    Transform strings to lowercase.

    Args:
        status (str | bool): The status of the power cut.

    Returns:
        str | None: cleaned status string or None if invalid.
    '''
    if status is None:
        return None

    if isinstance(status, bool):
        return "planned" if status else "unplanned"

    if isinstance(status, str):
        return status.strip().lower()

    return None


def transform_field(json_input: dict, field_name: str, transform_fn: callable = None) -> str | None:
    '''
    Generic field transformation helper.

    Args:
        json_input (dict): The input JSON record.
        field_name (str): The name of the field to transform.
        transform_fn (callable, optional): The transformation function to apply.
            If None, converts datetime to ISO format.

    Returns:
        str | None: Transformed field value or None if invalid.
    '''
    if field_name not in json_input:
        logger.warning("Missing %s field. Skipping record.", field_name)
        return None

    if transform_fn:
        field_value = transform_fn(
            json_input.get(field_name, None))
    else:
        field_value = json_input.get(field_name, None)
        if hasattr(field_value, 'isoformat'):
            field_value = field_value.isoformat()

    if not field_value:
        logger.warning(
            "No valid %s found. Skipping record.", field_name)
        return None

    return field_value


def main_transform(json_list_input: list[dict]) -> list[dict]:
    '''
    Main transformation function

    Args:
        json_list_input (list[dict]): List of input JSON records.

    Returns:
        list[dict]: List of transformed JSON records.
    '''
    fields = [
        ('affected_postcodes', transform_postcode_list),
        ('outage_date', None),
        ('source_provider', transform_source_provider),
        ('status', transform_status),
        ('recording_time', None),
    ]

    transformed_list = []

    for json_input in json_list_input:
        transformed = {}
        valid = True

        for field_name, transform_fn in fields:
            value = transform_field(json_input, field_name, transform_fn)
            if not value:
                valid = False
                break
            transformed[field_name] = value

        if valid:
            transformed_list.append(transformed)

    return transformed_list
