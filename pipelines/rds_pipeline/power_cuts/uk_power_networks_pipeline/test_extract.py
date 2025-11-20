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
                {"fields": {"postcodesaffected": "RH7 6", "powercuttype": "Planned"}},
                {"fields": {"postcodesaffected": "BN20 9", "powercuttype": "Unplanned"}}
            ]
        }

        # Act
        result = parse_records(raw_data)

        # Assert
        assert len(result) == 2
        assert result[0]["postcodesaffected"] == "RH7 6"

    @pytest.mark.parametrize("raw_data,expected", [
        (None, []),
        ({}, []),
        ({"results": []}, []),
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
            "postcodesaffected": "RH7 6",
            "creationdatetime": "2025-10-30T08:58:47",
            "powercuttype": "Planned"
        }

        # Act
        result = validate_record(record)

        # Assert
        assert result is True

    @pytest.mark.parametrize("record,expected", [
        ({"creationdatetime": "2025-10-30T08:58:47"}, False),
        ({"postcodesaffected": "", "creationdatetime": "2025-10-30T08:58:47"}, False),
        ({"postcodesaffected": None, "creationdatetime": "2025-10-30T08:58:47"}, False),
        ({"postcodesaffected": "RH7 6"}, False),
        ({"postcodesaffected": "RH7 6", "creationdatetime": ""}, False),
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
        ("RH7 6", "RH7 6"),
        ("PE17 1;PE26 1;PE26 2", "PE17 1;PE26 1;PE26 2"),
        ("  CM8 3  ", "CM8 3"),
    ])
    def test_transform_record_handles_postcode_formats(self, input_postcodes, expected_output):
        """Test transformation handles various postcode formats."""
        # Arrange
        record = {
            "postcodesaffected": input_postcodes,
            "creationdatetime": "2025-10-30T08:58:47",
            "powercuttype": "Planned"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["affected_postcodes"] == expected_output

    def test_transform_record_output_structure(self):
        """Test transformed record has all expected keys and correct values."""
        # Arrange
        record = {
            "postcodesaffected": "RH7 6",
            "creationdatetime": "2025-10-30T08:58:47",
            "powercuttype": "Planned"
        }
        expected_keys = {"source_provider", "status", "outage_date",
                         "recording_time", "affected_postcodes"}

        # Act
        result = transform_record(record)

        # Assert
        assert set(result.keys()) == expected_keys
        assert result["source_provider"] == "UK Power Networks"
        assert result["status"] == "Planned"
        assert result["outage_date"] == "2025-10-30T08:58:47"
        assert "T" in result["recording_time"]
