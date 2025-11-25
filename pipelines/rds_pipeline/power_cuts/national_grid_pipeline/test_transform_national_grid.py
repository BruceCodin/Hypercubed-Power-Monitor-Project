# pylint: skip-file
# pragma: no cover
import pytest
from transform_national_grid import (
    parse_postcodes,
    standardize_status,
    normalize_datetime,
    transform_data_national_grid
)


class TestPostcodeParsing:
    """Tests for parsing postcode strings"""

    @pytest.mark.parametrize("input_string,expected", [
        ("SA34 0TH", ["SA34 0TH"]),
        ("SA34 0TH, SA34 0UY", ["SA34 0TH", "SA34 0UY"]),
        ("SA34 0TH, SA34 0UY, SA34 0XD", ["SA34 0TH", "SA34 0UY", "SA34 0XD"]),
        ("  SA34 0TH  ,  SA34 0UY  ", ["SA34 0TH", "SA34 0UY"]),
        ("", []),
        ("   ", []),
    ])
    def test_parse_postcodes_handles_various_formats(self, input_string, expected):
        """Test postcode parsing handles single, multiple, and edge cases."""
        # Act
        result = parse_postcodes(input_string)

        # Assert
        assert result == expected


class TestStatusStandardization:
    """Tests for standardizing status values"""

    @pytest.mark.parametrize("input_status,expected", [
        ("false", "unplanned"),
        ("true", "planned"),
        ("False", "unplanned"),
        ("True", "planned"),
        ("", "unplanned"),
        (None, "unplanned"),
    ])
    def test_standardize_status_converts_correctly(self, input_status, expected):
        """Test status standardization for various input values."""
        # Act
        result = standardize_status(input_status)

        # Assert
        assert result == expected


class TestDatetimeNormalization:
    """Tests for normalizing datetime formats"""

    @pytest.mark.parametrize("input_datetime,expected", [
        ("2025-11-20T10:13:00", "2025-11-20T10:13:00"),
        ("2025-11-20T10:13:00.123456", "2025-11-20T10:13:00"),
        ("2025-11-20T10:13:00.123456+00:00", "2025-11-20T10:13:00+00:00"),
        ("2025-11-20T10:13:00Z", "2025-11-20T10:13:00+00:00"),
    ])
    def test_normalize_datetime_removes_microseconds(self, input_datetime, expected):
        """Test datetime normalization keeps ISO format and removes microseconds."""
        # Act
        result = normalize_datetime(input_datetime)

        # Assert
        assert result == expected

    def test_normalize_datetime_handles_empty_string(self):
        """Test datetime normalization handles empty input."""
        # Act
        result = normalize_datetime("")

        # Assert
        assert result == ""


class TestFullTransformation:
    """Tests for complete transformation pipeline"""

    def test_transform_data_national_grid_single_postcode(self):
        """Test transformation with single postcode record."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'SA34 0TH',
            'outage_date': '2025-11-20T10:13:00',
            'status': 'false',
            'source_provider': 'National Grid',
            'recording_time': '2025-11-20T11:20:47.986397'
        }]

        # Act
        result = transform_data_national_grid(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['SA34 0TH']
        assert result[0]['status'] == 'unplanned'
        assert result[0]['outage_date'] == '2025-11-20T10:13:00'
        assert result[0]['recording_time'] == '2025-11-20T11:20:47'

    def test_transform_data_national_grid_multiple_postcodes(self):
        """Test transformation converts comma-separated postcodes to list."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'SA34 0TH, SA34 0UY, SA34 0XD',
            'outage_date': '2025-11-20T10:13:00',
            'status': 'true',
            'source_provider': 'National Grid',
            'recording_time': '2025-11-20T11:20:47.986397'
        }]

        # Act
        result = transform_data_national_grid(extracted_data)

        # Assert
        assert len(result) == 1
        assert len(result[0]['affected_postcodes']) == 3
        assert result[0]['affected_postcodes'] == [
            'SA34 0TH', 'SA34 0UY', 'SA34 0XD']
        assert result[0]['status'] == 'planned'

    def test_transform_data_national_grid_skips_invalid_records(self):
        """Test transformation skips records with no postcodes."""
        # Arrange
        extracted_data = [
            {
                'affected_postcodes': '',
                'outage_date': '2025-11-20T10:13:00',
                'status': 'false',
                'source_provider': 'National Grid',
                'recording_time': '2025-11-20T11:20:47'
            },
            {
                'affected_postcodes': 'SA34 0TH',
                'outage_date': '2025-11-20T10:13:00',
                'status': 'false',
                'source_provider': 'National Grid',
                'recording_time': '2025-11-20T11:20:47'
            }
        ]

        # Act
        result = transform_data_national_grid(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['SA34 0TH']

    def test_transform_data_national_grid_handles_empty_input(self):
        """Test transformation handles empty input list."""
        # Act
        result = transform_data_national_grid([])

        # Assert
        assert result == []
