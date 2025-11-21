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
        ("", []),
    ])
    def test_parse_postcodes_handles_various_formats(self, input_string, expected):
        """Test postcode parsing handles comma-separated with leading spaces."""
        result = parse_postcodes(input_string)
        assert result == expected


class TestStatusStandardization:
    """Tests for standardizing status values"""

    @pytest.mark.parametrize("input_status,expected", [
        ("Planned Work", "planned"),
        ("Current Fault", "unplanned"),
        ("", "unplanned"),
    ])
    def test_standardize_status_converts_correctly(self, input_status, expected):
        """Test status standardization for SP Northwest status values."""
        result = standardize_status(input_status)
        assert result == expected


class TestDatetimeNormalization:
    """Tests for normalizing datetime formats"""

    @pytest.mark.parametrize("input_datetime,expected", [
        ("2025-11-28T00:00:00", "2025-11-28T00:00:00"),
        ("2025-11-21T13:14:03.870246", "2025-11-21T13:14:03"),
        ("", ""),
    ])
    def test_normalize_datetime_removes_microseconds(self, input_datetime, expected):
        """Test datetime normalization removes microseconds."""
        result = normalize_datetime(input_datetime)
        assert result == expected


class TestFullTransformation:
    """Tests for complete transformation pipeline"""

    def test_transform_single_postcode(self):
        """Test transformation with single postcode record."""
        extracted_data = [{
            'affected_postcodes': ' BL7 9JT',
            'outage_date': '2025-11-21T05:35:57',
            'status': 'Current Fault',
            'source_provider': 'SP Electricity North West',
            'recording_time': '2025-11-21T13:25:29.123456'
        }]

        result = transform_data_sp_northwest(extracted_data)

        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['BL7 9JT']
        assert result[0]['status'] == 'unplanned'
        assert result[0]['recording_time'] == '2025-11-21T13:25:29'

    def test_transform_multiple_postcodes(self):
        """Test transformation with comma-separated postcodes."""
        extracted_data = [{
            'affected_postcodes': ' CA7 5HX, CA7 5HY, CA7 5HT',
            'outage_date': '2025-11-28T00:00:00',
            'status': 'Planned Work',
            'source_provider': 'SP Electricity North West',
            'recording_time': '2025-11-21T13:14:03.870248'
        }]

        result = transform_data_sp_northwest(extracted_data)

        assert len(result) == 1
        assert result[0]['affected_postcodes'] == [
            'CA7 5HX', 'CA7 5HY', 'CA7 5HT']
        assert result[0]['status'] == 'planned'

    def test_transform_handles_empty_input(self):
        """Test transformation handles empty input list."""
        result = transform_data_sp_northwest([])
        assert result == []
