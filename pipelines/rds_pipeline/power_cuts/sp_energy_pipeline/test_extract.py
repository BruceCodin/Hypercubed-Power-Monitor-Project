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
        # Another single
        (["EH6 5"], "EH6 5"),
    ])
    def test_transform_record_handles_postcode_list(self, input_postcodes, expected_output):
        """Test transformation converts postcode list to comma-separated string."""
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

    def test_transform_record_handles_non_list_postcode(self):
        """Test transformation handles non-list postcode gracefully."""
        # Arrange
        record = {
            "postcode_sector": "TD12 4",  # String instead of list
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["affected_postcodes"] == "TD12 4"

    @pytest.mark.parametrize("status", [
        "Awaiting",
        "In Progress",
        "Restored",
    ])
    def test_transform_record_preserves_status(self, status):
        """Test transformation preserves status field."""
        # Arrange
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": status
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["status"] == status

    def test_transform_record_preserves_outage_date(self):
        """Test transformation preserves outage_date as string."""
        # Arrange
        outage_date = "2025-11-17T08:24:11+00:00"
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": outage_date,
            "status": "Awaiting"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["outage_date"] == outage_date
        assert isinstance(result["outage_date"], str)

    def test_transform_record_adds_source_provider(self):
        """Test transformation adds source_provider field."""
        # Arrange
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["source_provider"] == "SP Energy Networks"

    def test_transform_record_adds_recording_time(self):
        """Test transformation adds recording_time timestamp."""
        # Arrange
        record = {
            "postcode_sector": ["TD12 4"],
            "date_of_reported_fault": "2025-11-17T08:24:11+00:00",
            "status": "Awaiting"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert "recording_time" in result
        assert isinstance(result["recording_time"], str)
        assert "T" in result["recording_time"]

    def test_transform_record_output_structure(self):
        """Test transformed record has all expected keys."""
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
