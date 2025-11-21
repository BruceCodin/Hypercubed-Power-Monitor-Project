# pylint: skip-file
# test_extract.py
import pytest
from extract_national_grid import (
    parse_records,
    validate_record,
    transform_record
)


class TestRecordParsing:
    """Tests for parsing records from API response"""

    @pytest.mark.parametrize("total,record_count,expected_length", [
        (2, 2, 2),
        (0, 0, 0),
        (5, 5, 5),
    ])
    def test_parse_records_extracts_correct_count(self, total, record_count, expected_length):
        """Test parsing extracts correct number of records."""
        # Arrange
        raw_data = {
            "success": True,
            "result": {
                "total": total,
                "records": [{"Postcodes": f"TEST{i}"} for i in range(record_count)]
            }
        }

        # Act
        result = parse_records(raw_data)

        # Assert
        assert len(result) == expected_length

    @pytest.mark.parametrize("raw_data,expected", [
        (None, []),
        ({}, []),
        ({"success": False}, []),
        ({"success": True, "result": {}}, []),
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
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Planned": "false"
        }

        # Act
        result = validate_record(record)

        # Assert
        assert result is True

    @pytest.mark.parametrize("record,expected", [
        ({"Start Time": "2025-11-14T15:33:00", "Planned": "false"}, False),
        ({"Postcodes": "", "Start Time": "2025-11-14T15:33:00", "Planned": "false"}, False),
        ({"Postcodes": "   ", "Start Time": "2025-11-14T15:33:00",
         "Planned": "false"}, False),
        ({"Postcodes": None, "Start Time": "2025-11-14T15:33:00", "Planned": "false"}, False),
        ({"Postcodes": "EX37 9TB", "Planned": "false"}, False),
        ({"Postcodes": "EX37 9TB", "Start Time": "", "Planned": "false"}, False),
        ({"Postcodes": "EX37 9TB", "Start Time": "   ", "Planned": "false"}, False),
        ({"Planned": "false"}, False),
    ])
    def test_validate_record_rejects_invalid_data(self, record, expected):
        """Test validation correctly rejects records with missing/invalid required fields."""
        # Act
        result = validate_record(record)

        # Assert
        assert result == expected


class TestRecordTransformation:
    """Tests for transforming raw records to clean format"""

    @pytest.mark.parametrize("input_postcode,expected_postcode", [
        ("EX37 9TB", "EX37 9TB"),
        ("  EX37 9TB  ", "EX37 9TB"),
        ("BS20 6NB", "BS20 6NB"),
        ("  BA1 6TL  ", "BA1 6TL"),
    ])
    def test_transform_record_cleans_postcode(self, input_postcode, expected_postcode):
        """Test transformation strips whitespace from postcodes."""
        # Arrange
        record = {
            "Postcodes": input_postcode,
            "Start Time": "2025-11-14T15:33:00",
            "Planned": "false"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["affected_postcodes"] == expected_postcode

    @pytest.mark.parametrize("start_time", [
        "2025-11-14T15:33:00",
        "2025-10-22T08:59:00",
        "2025-11-15T00:00:00",
    ])
    def test_transform_record_preserves_outage_date_as_string(self, start_time):
        """Test transformation keeps outage_date as string without modification."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": start_time,
            "Planned": "false"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["outage_date"] == start_time
        assert isinstance(result["outage_date"], str)

    @pytest.mark.parametrize("planned", [
        "false",
        "true",
    ])
    def test_transform_record_preserves_planned(self, planned):
        """Test transformation preserves planned field."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Planned": planned
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["status"] == planned

    def test_transform_record_adds_source_provider(self):
        """Test transformation adds source_provider field."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Planned": "false"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["source_provider"] == "National Grid"

    def test_transform_record_adds_recording_time(self):
        """Test transformation adds recording_time timestamp."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Planned": "false"
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
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Planned": "false"
        }
        expected_keys = {"affected_postcodes", "outage_date",
                         "status", "source_provider", "recording_time"}

        # Act
        result = transform_record(record)

        # Assert
        assert set(result.keys()) == expected_keys
