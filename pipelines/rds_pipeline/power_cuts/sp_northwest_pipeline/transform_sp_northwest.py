# pylint: disable=W1203, C0301
"""Transform SP Electricity North West extracted power cuts data to standardized format"""
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
        postcode_string: String of postcodes (e.g., " BB7 3ED, BB7 3EE")
        
    Returns:
        List of cleaned postcode strings
    """
    if not postcode_string or not postcode_string.strip():
        return []

    # Split by comma and strip whitespace from each postcode
    postcodes = [pc.strip() for pc in postcode_string.split(',')]

    # Filter out empty strings
    return [pc for pc in postcodes if pc]


def standardize_status(status_value: str) -> str:
    """
    Convert SP Northwest status field to standardized status.
    
    Args:
        status_value: String value from 'status' field
                      ('Current Fault' or 'Planned Work')
        
    Returns:
        'planned' or 'unplanned'
    """
    if not status_value:
        return 'unplanned'

    # Normalize to lowercase for comparison
    status_lower = status_value.lower().strip()

    # Map known status values
    if 'planned' in status_lower:
        return 'planned'
    if 'fault' in status_lower or 'current' in status_lower:
        return 'unplanned'

    # Default to unplanned for unknown values
    return 'unplanned'


def normalize_datetime(iso_string: str) -> str:
    """
    Normalize datetime to consistent ISO 8601 format for PostgreSQL.
    Removes microseconds but keeps ISO format (YYYY-MM-DDTHH:MM:SS).
    
    Args:
        iso_string: ISO format datetime (e.g., '2025-11-28T00:00:00')
        
    Returns:
        Normalized ISO format datetime (e.g., '2025-11-28T00:00:00')
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


def transform_data_sp_northwest(raw_extracted_data: List[Dict]) -> List[Dict]:
    """
    Transform SP Electricity North West extracted data to standardized format.
    
    Args:
        raw_extracted_data: List of dicts from extract_power_cut_data()
        
    Returns:
        List of transformed dicts ready for database load
        
    Expected output format:
    {
        "affected_postcodes": ["postcode1", "postcode2"],
        "outage_date": "YYYY-MM-DDTHH:MM:SS" (ISO 8601),
        "source_provider": str,
        "status": "planned" or "unplanned",
        "recording_time": "YYYY-MM-DDTHH:MM:SS" (ISO 8601)
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

            # Standardize status (SP Northwest uses 'Current Fault' or 'Planned Work')
            status = standardize_status(record.get('status', ''))

            # Normalize datetime formats (keep ISO 8601, remove microseconds)
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
    from extract_sp_northwest import extract_data_sp_northwest

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting SP Electricity North West power cuts transformation...")

    # Extract data
    extracted_data = extract_data_sp_northwest()

    # Transform data
    transformed_data = transform_data_sp_northwest(extracted_data)

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
