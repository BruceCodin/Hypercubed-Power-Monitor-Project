# pylint: skip-file
# test_transform.py
import pytest
from transform_sp_en import (
    parse_postcodes,
    standardize_status,
    normalize_datetime,
    transform_data_sp_en
)


class TestPostcodeParsing:
    """Tests for parsing postcode strings"""

    @pytest.mark.parametrize("input_string,expected", [
        ("G66 4", ["G66 4"]),
        ("G66 4, G71 8", ["G66 4", "G71 8"]),
        ("LL65 3, LL65 4, LL71 7", ["LL65 3", "LL65 4", "LL71 7"]),
        ("  G66 4  ,  G71 8  ", ["G66 4", "G71 8"]),
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
        (False, "unplanned"),
        (True, "planned"),
        ("true", "planned"),
        ("false", "unplanned"),
        ("", "unplanned"),
        (None, "unplanned"),
    ])
    def test_standardize_status_converts_correctly(self, input_status, expected):
        """Test status standardization for boolean and string values."""
        # Act
        result = standardize_status(input_status)

        # Assert
        assert result == expected


class TestDatetimeNormalization:
    """Tests for normalizing datetime formats"""

    @pytest.mark.parametrize("input_datetime,expected", [
        ("2025-11-20T12:03:47+00:00", "2025-11-20T12:03:47+00:00"),
        ("2025-11-20T12:03:47.123456+00:00", "2025-11-20T12:03:47+00:00"),
        ("2025-11-20T10:13:00", "2025-11-20T10:13:00"),
        ("2025-11-20T10:13:00.999999", "2025-11-20T10:13:00"),
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

    def test_transform_data_sp_en_single_postcode(self):
        """Test transformation with single postcode record."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'G66 4',
            'outage_date': '2025-11-20T12:03:47+00:00',
            'status': False,
            'source_provider': 'SP Energy Networks',
            'recording_time': '2025-11-20T13:29:27.123456'
        }]

        # Act
        result = transform_data_sp_en(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['G66 4']
        assert result[0]['status'] == 'unplanned'
        assert result[0]['outage_date'] == '2025-11-20T12:03:47+00:00'
        assert result[0]['recording_time'] == '2025-11-20T13:29:27'

    def test_transform_data_sp_en_multiple_postcodes(self):
        """Test transformation with multiple comma-separated postcodes."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'LL65 3, LL65 4, LL71 7',
            'outage_date': '2025-11-20T09:00:00+00:00',
            'status': True,
            'source_provider': 'SP Energy Networks',
            'recording_time': '2025-11-20T13:29:27.123456'
        }]

        # Act
        result = transform_data_sp_en(extracted_data)

        # Assert
        assert len(result) == 1
        assert len(result[0]['affected_postcodes']) == 3
        assert result[0]['affected_postcodes'] == [
            'LL65 3', 'LL65 4', 'LL71 7']
        assert result[0]['status'] == 'planned'

    def test_transform_data_sp_en_skips_invalid_records(self):
        """Test transformation skips records with no postcodes."""
        # Arrange
        extracted_data = [
            {
                'affected_postcodes': '',
                'outage_date': '2025-11-20T12:03:47+00:00',
                'status': False,
                'source_provider': 'SP Energy Networks',
                'recording_time': '2025-11-20T13:29:27'
            },
            {
                'affected_postcodes': 'G66 4',
                'outage_date': '2025-11-20T12:03:47+00:00',
                'status': False,
                'source_provider': 'SP Energy Networks',
                'recording_time': '2025-11-20T13:29:27'
            }
        ]

        # Act
        result = transform_data_sp_en(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['G66 4']

    def test_transform_data_sp_en_handles_empty_input(self):
        """Test transformation handles empty input list."""
        # Act
        result = transform_data_sp_en([])

        # Assert
        assert result == []
