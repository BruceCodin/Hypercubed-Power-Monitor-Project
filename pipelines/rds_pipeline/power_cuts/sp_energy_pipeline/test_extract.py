# pylint: skip-file
# test_extract.py
import pytest
from extract import (
    parse_records,
    validate_record,
    transform_record
)


class TestRecordParsing:
    """Tests for parsing records from API response"""

    def test_parse_records_extracts_from_results(self):
        """Test parsing extracts records from Opendatasoft 'results' structure."""
        # Arrange
        raw_data = {
            "total_count": 2,
            "results": [
                {"fields": {"postcode_sector": [
                    "TD12 4"], "status": "Awaiting"}},
                {"fields": {"postcode_sector": [
                    "EH6 5"], "status": "In Progress"}}
            ]
        }

        # Act
        result = parse_records(raw_data)

        # Assert
        assert len(result) == 2
        assert result[0]["postcode_sector"] == ["TD12 4"]

    @pytest.mark.parametrize("raw_data,expected", [
        (None, []),                                    # None input
        ({}, []),                                      # Empty dict
        ({"results": []}, []),                         # Empty results
    ])
    def test_parse_records_handles_invalid_input(self, raw_data, expected):
        """Test parsing handles various invalid inputs."""
        # Act
        result = parse_records(raw_data)

        # Assert
        assert result == expected


class TestRecordValidation:
    """Tests for validating individual power cut records"""

    def test_validate_record_with_valid_data(self):
        """Test validation passes for record with all required fields."""
        # Arrange
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }

        # Act
        result = validate_record(record)

        # Assert
        assert result is True

    @pytest.mark.parametrize("record,expected", [
        # Missing postcode_sector
        ({"date_of_reported_fault": "2025-11-17T08:24:11+00:00"}, False),
        # Empty postcode_sector
        ({"postcode_sector": [], "date_of_reported_fault": "2025-11-17T08:24:11+00:00"}, False),
        # None postcode_sector
        ({"postcode_sector": None,
         "date_of_reported_fault": "2025-11-17T08:24:11+00:00"}, False),
        # Missing date_of_reported_fault
        ({"postcode_sector": ["TD12 4"]}, False),
        # Empty date_of_reported_fault
        ({"postcode_sector": ["TD12 4"], "date_of_reported_fault": ""}, False),
        # Both missing
        ({}, False),
    ])
    def test_validate_record_rejects_invalid_data(self, record, expected):
        """Test validation correctly rejects records with missing/invalid required fields."""
        # Act
        result = validate_record(record)

        # Assert
        assert result == expected


class TestRecordTransformation:
    """Tests for transforming raw records to clean format"""

    @pytest.mark.parametrize("input_postcodes,expected_output", [
        # Single postcode
        (["TD12 4"], "TD12 4"),
        # Multiple postcodes
        (["CH7 6", "CH7 2", "CH7 4"], "CH7 6, CH7 2, CH7 4"),
        # Non-list postcode
        ("TD12 4", "TD12 4"),
    ])
    def test_transform_record_handles_postcode_formats(self, input_postcodes, expected_output):
        """Test transformation handles both list and string postcodes."""
        # Arrange
        record = {
            "postcode_sector": input_postcodes,
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["affected_postcodes"] == expected_output

    def test_transform_record_output_structure(self):
        """Test transformed record has all expected keys and correct values."""
        # Arrange
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }
        expected_keys = {"source_provider", "status", "outage_date",
                         "recording_time", "affected_postcodes"}

        # Act
        result = transform_record(record)

        # Assert
        assert set(result.keys()) == expected_keys
        assert result["source_provider"] == "SP Energy Networks"
        assert result["status"] == "Awaiting"
        assert result["outage_date"] == "2025-11-17T08:24:11+00:00"
        assert "T" in result["recording_time"]
