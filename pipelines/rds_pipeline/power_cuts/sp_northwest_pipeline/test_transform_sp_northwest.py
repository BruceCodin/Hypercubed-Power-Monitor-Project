# pylint: skip-file
# test_transform_sp_northwest.py
import pytest
from transform_sp_northwest import (
    parse_postcodes,
    standardize_status,
    normalize_datetime,
    transform_data_sp_northwest
)


class TestPostcodeParsing:
    """Tests for parsing postcode strings"""

    @pytest.mark.parametrize("input_string,expected", [
        (" BB7 3ED", ["BB7 3ED"]),
        (" BB7 3ED, BB7 3EE", ["BB7 3ED", "BB7 3EE"]),
        (" CA7 5HX, CA7 5HY, CA7 5HT", ["CA7 5HX", "CA7 5HY", "CA7 5HT"]),
        ("  BB7 3ED  ,  BB7 3EE  ", ["BB7 3ED", "BB7 3EE"]),
        ("", []),
        ("   ", []),
    ])
    def test_parse_postcodes_handles_various_formats(self, input_string, expected):
        """Test postcode parsing handles comma-separated with leading spaces."""
        # Act
        result = parse_postcodes(input_string)

        # Assert
        assert result == expected


class TestStatusStandardization:
    """Tests for standardizing status values"""

    @pytest.mark.parametrize("input_status,expected", [
        ("Planned Work", "planned"),
        ("Current Fault", "unplanned"),
        ("planned work", "planned"),
        ("CURRENT FAULT", "unplanned"),
        ("", "unplanned"),
        (None, "unplanned"),
    ])
    def test_standardize_status_converts_correctly(self, input_status, expected):
        """Test status standardization for SP Northwest status values."""
        # Act
        result = standardize_status(input_status)

        # Assert
        assert result == expected


class TestDatetimeNormalization:
    """Tests for normalizing datetime formats"""

    @pytest.mark.parametrize("input_datetime,expected", [
        ("2025-11-28T00:00:00", "2025-11-28T00:00:00"),
        ("2025-11-21T13:14:03.870246", "2025-11-21T13:14:03"),
        ("2025-11-21T05:35:57+00:00", "2025-11-21T05:35:57+00:00"),
        ("2025-11-21T10:13:19.123456+00:00", "2025-11-21T10:13:19+00:00"),
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

    def test_transform_single_postcode(self):
        """Test transformation with single postcode record."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': ' BL7 9JT',
            'outage_date': '2025-11-21T05:35:57',
            'status': 'Current Fault',
            'source_provider': 'SP Electricity North West',
            'recording_time': '2025-11-21T13:25:29.123456'
        }]

        # Act
        result = transform_data_sp_northwest(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['BL7 9JT']
        assert result[0]['status'] == 'unplanned'
        assert result[0]['outage_date'] == '2025-11-21T05:35:57'
        assert result[0]['recording_time'] == '2025-11-21T13:25:29'

    def test_transform_multiple_postcodes(self):
        """Test transformation with comma-separated postcodes."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': ' CA7 5HX, CA7 5HY, CA7 5HT, CA7 5HZ',
            'outage_date': '2025-11-28T00:00:00',
            'status': 'Planned Work',
            'source_provider': 'SP Electricity North West',
            'recording_time': '2025-11-21T13:14:03.870248'
        }]

        # Act
        result = transform_data_sp_northwest(extracted_data)

        # Assert
        assert len(result) == 1
        assert len(result[0]['affected_postcodes']) == 4
        assert result[0]['affected_postcodes'] == [
            'CA7 5HX', 'CA7 5HY', 'CA7 5HT', 'CA7 5HZ']
        assert result[0]['status'] == 'planned'

    def test_transform_handles_leading_spaces(self):
        """Test transformation correctly strips leading spaces from postcodes."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': ' BB7 3ED, BB7 3EE',
            'outage_date': '2025-11-28T00:00:00',
            'status': 'Planned Work',
            'source_provider': 'SP Electricity North West',
            'recording_time': '2025-11-21T13:14:03'
        }]

        # Act
        result = transform_data_sp_northwest(extracted_data)

        # Assert
        assert result[0]['affected_postcodes'] == ['BB7 3ED', 'BB7 3EE']
        # Ensure no leading spaces
        assert all(not pc.startswith(' ')
                   for pc in result[0]['affected_postcodes'])

    def test_transform_skips_invalid_records(self):
        """Test transformation skips records with no postcodes."""
        # Arrange
        extracted_data = [
            {
                'affected_postcodes': '',
                'outage_date': '2025-11-28T00:00:00',
                'status': 'Planned Work',
                'source_provider': 'SP Electricity North West',
                'recording_time': '2025-11-21T13:14:03'
            },
            {
                'affected_postcodes': ' BB7 3ED',
                'outage_date': '2025-11-28T00:00:00',
                'status': 'Planned Work',
                'source_provider': 'SP Electricity North West',
                'recording_time': '2025-11-21T13:14:03'
            }
        ]

        # Act
        result = transform_data_sp_northwest(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['BB7 3ED']

    def test_transform_handles_empty_input(self):
        """Test transformation handles empty input list."""
        # Act
        result = transform_data_sp_northwest([])

        # Assert
        assert result == []
