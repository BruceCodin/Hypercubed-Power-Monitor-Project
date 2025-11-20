# pylint: disable=W1203, C0301
"""Transform National Grid extracted power cuts data to standardized format"""
import logging
from typing import List, Dict
from datetime import datetime

# Logging configuration
logger = logging.getLogger(__name__)


def parse_postcodes(postcode_string: str) -> List[str]:
    """
    Parse postcode string into list of individual postcodes.
    Handles comma-separated postcodes and strips whitespace.
    
    Args:
        postcode_string: String of postcodes (e.g., "SA34 0TH, SA34 0UY, SA34 0XD")
        
    Returns:
        List of cleaned postcode strings
    """
    if not postcode_string or not postcode_string.strip():
        return []

    # Split by comma and strip whitespace from each postcode
    postcodes = [pc.strip() for pc in postcode_string.split(',')]

    # Filter out empty strings
    return [pc for pc in postcodes if pc]


def standardize_status(planned_value: str) -> str:
    """
    Convert National Grid 'Planned' field to standardized status.
    
    Args:
        planned_value: String value from 'Planned' field ('true' or 'false')
        
    Returns:
        'planned' or 'unplanned'
    """
    if not planned_value:
        return 'unplanned'

    # Handle string representation
    if isinstance(planned_value, str):
        if planned_value.lower() == 'true':
            return 'planned'

    # Handle boolean (in case API returns actual boolean)
    if isinstance(planned_value, bool) and planned_value:
        return 'planned'

    return 'unplanned'


def normalize_datetime(iso_string: str) -> str:
    """
    Normalize datetime to consistent ISO 8601 format for PostgreSQL.
    Removes microseconds but keeps ISO format (YYYY-MM-DDTHH:MM:SS).
    
    Args:
        iso_string: ISO format datetime (e.g., '2025-11-20T10:13:00.123456')
        
    Returns:
        Normalized ISO format datetime (e.g., '2025-11-20T10:13:00')
    """
    if not iso_string:
        return ''

    try:
        # Replace 'Z' with '+00:00' for fromisoformat compatibility
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        # Return ISO format with second precision (removes microseconds)
        return dt.replace(microsecond=0).isoformat()

    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to normalize datetime '{iso_string}': {e}")
        return iso_string


def transform_power_cut_data(raw_extracted_data: List[Dict]) -> List[Dict]:
    """
    Transform National Grid extracted data to standardized format.
    
    Converts comma-separated postcode strings to lists while standardizing
    datetime and status fields.
    
    Args:
        raw_extracted_data: List of dicts from extract_power_cut_data()
        
    Returns:
        List of transformed dicts ready for database load
        
    Expected output format:
    {
        "affected_postcodes": ["postcode1", "postcode2"],
        "outage_date": "YYYY-MM-DDTHH:MM:SS", 
        "source_provider": str,
        "status": "planned" or "unplanned",
        "recording_time": "YYYY-MM-DDTHH:MM:SS" 
    }
    """
    if not raw_extracted_data:
        logger.warning("No extracted data to transform")
        return []

    transformed_records = []

    for record in raw_extracted_data:
        try:
            # Parse postcodes into list
            postcodes = parse_postcodes(record.get('affected_postcodes', ''))

            if not postcodes:
                logger.warning(
                    f"Skipping record with no valid postcodes: {record}")
                continue

            # Standardize status
            status = standardize_status(record.get('status', ''))

            # Convert datetime formats (keep ISO 8601, remove microseconds)
            outage_date = normalize_datetime(record.get('outage_date', ''))
            recording_time = normalize_datetime(
                record.get('recording_time', ''))

            # Create transformed record
            transformed_record = {
                'affected_postcodes': postcodes,  # Now a list
                'outage_date': outage_date,
                'source_provider': record.get('source_provider', ''),
                'status': status,
                'recording_time': recording_time
            }

            transformed_records.append(transformed_record)

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Failed to transform record {record}: {e}")
            continue

    logger.info(
        f"Transformed {len(raw_extracted_data)} extracted records into {len(transformed_records)} standardized records")

    return transformed_records


if __name__ == "__main__":
    # Example usage for local testing
    from pprint import pprint
    from extract import extract_power_cut_data

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting National Grid power cuts transformation...")

    # Extract data
    extracted_data = extract_power_cut_data()

    # Transform data
    transformed_data = transform_power_cut_data(extracted_data)

    if transformed_data:
        logger.info(
            f"Transformation complete! Transformed {len(transformed_data)} records")
        print("\n" + "="*80)
        print(
            f"Sample of first {min(5, len(transformed_data))} transformed records:")
        print("="*80)
        pprint(transformed_data[:5])
    else:
        logger.warning("No data transformed")
