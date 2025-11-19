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

    @pytest.mark.parametrize("total,record_count,expected_length", [
        (2, 2, 2),      # Normal case
        (0, 0, 0),      # Empty response
        (5, 5, 5),      # Multiple records
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
        (None, []),                                          # None input
        ({}, []),                                            # Empty dict
        ({"success": False}, []),                            # Failed response
        ({"success": True, "result": {}}, []),              # Missing records key
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
            "Status": "In Progress"
        }

        # Act
        result = validate_record(record)

        # Assert
        assert result is True

    @pytest.mark.parametrize("record,expected", [
        # Missing postcodes
        ({"Start Time": "2025-11-14T15:33:00", "Status": "In Progress"}, False),
        # Empty postcodes
        ({"Postcodes": "", "Start Time": "2025-11-14T15:33:00",
         "Status": "In Progress"}, False),
        # Whitespace only postcodes
        ({"Postcodes": "   ", "Start Time": "2025-11-14T15:33:00",
         "Status": "In Progress"}, False),
        # None postcodes
        ({"Postcodes": None, "Start Time": "2025-11-14T15:33:00",
         "Status": "In Progress"}, False),
        # Missing start time
        ({"Postcodes": "EX37 9TB", "Status": "In Progress"}, False),
        # Empty start time
        ({"Postcodes": "EX37 9TB", "Start Time": "", "Status": "In Progress"}, False),
        # Whitespace only start time
        ({"Postcodes": "EX37 9TB", "Start Time": "   ", "Status": "In Progress"}, False),
        # Both missing
        ({"Status": "In Progress"}, False),
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
        ("EX37 9TB", "EX37 9TB"),          # Normal postcode
        ("  EX37 9TB  ", "EX37 9TB"),      # With leading/trailing whitespace
        ("BS20 6NB", "BS20 6NB"),          # Different postcode
        ("  BA1 6TL  ", "BA1 6TL"),        # Another with whitespace
    ])
    def test_transform_record_cleans_postcode(self, input_postcode, expected_postcode):
        """Test transformation strips whitespace from postcodes."""
        # Arrange
        record = {
            "Postcodes": input_postcode,
            "Start Time": "2025-11-14T15:33:00",
            "Status": "In Progress"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["postcode"] == expected_postcode

    @pytest.mark.parametrize("start_time", [
        "2025-11-14T15:33:00",
        "2025-10-22T08:59:00",
        "2025-11-15T00:00:00",
    ])
    def test_transform_record_preserves_start_time_as_string(self, start_time):
        """Test transformation keeps start_time as string without modification."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": start_time,
            "Status": "In Progress"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["start_time"] == start_time
        assert isinstance(result["start_time"], str)

    @pytest.mark.parametrize("status", [
        "In Progress",
        "Awaiting",
        "Restored",
    ])
    def test_transform_record_preserves_status(self, status):
        """Test transformation preserves status field."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Status": status
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["status"] == status

    def test_transform_record_adds_data_source(self):
        """Test transformation adds data_source field."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Status": "In Progress"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert result["data_source"] == "national_grid"

    def test_transform_record_adds_extracted_at(self):
        """Test transformation adds extracted_at timestamp."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Status": "In Progress"
        }

        # Act
        result = transform_record(record)

        # Assert
        assert "extracted_at" in result
        assert isinstance(result["extracted_at"], str)
        # Verify it's ISO format (basic check - don't test datetime library)
        assert "T" in result["extracted_at"]

    def test_transform_record_output_structure(self):
        """Test transformed record has all expected keys."""
        # Arrange
        record = {
            "Postcodes": "EX37 9TB",
            "Start Time": "2025-11-14T15:33:00",
            "Status": "In Progress"
        }
        expected_keys = {"postcode", "start_time",
                         "status", "data_source", "extracted_at"}

        # Act
        result = transform_record(record)

        # Assert
        assert set(result.keys()) == expected_keys
