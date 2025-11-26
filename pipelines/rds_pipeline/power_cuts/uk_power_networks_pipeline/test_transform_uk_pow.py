# pylint: skip-file
# pragma: no cover
import pytest
from transform_uk_pow import (
    parse_postcodes,
    standardize_status,
    normalize_datetime,
    transform_data_uk_pow
)


class TestPostcodeParsing:
    """Tests for parsing postcode strings"""

    @pytest.mark.parametrize("input_string,expected", [
        ("IP28 8", ["IP28 8"]),
        ("IP28 8;IP29 4", ["IP28 8", "IP29 4"]),
        ("IP28 8;IP29 4;IP30 0", ["IP28 8", "IP29 4", "IP30 0"]),
        ("  IP28 8  ;  IP29 4  ", ["IP28 8", "IP29 4"]),
        ("", []),
        ("   ", []),
    ])
    def test_parse_postcodes_handles_semicolon_format(self, input_string, expected):
        """Test postcode parsing handles semicolon-separated format."""
        # Act
        result = parse_postcodes(input_string)

        # Assert
        assert result == expected


class TestStatusStandardization:
    """Tests for standardizing status values"""

    @pytest.mark.parametrize("input_status,expected", [
        ("Planned", "planned"),
        ("Unplanned", "unplanned"),
        ("Restored", "unknown"),
        ("Multiple", "unknown"),
        ("planned", "planned"),
        ("UNPLANNED", "unplanned"),
        ("", "unknown"),
        (None, "unknown"),
    ])
    def test_standardize_status_converts_correctly(self, input_status, expected):
        """Test status standardization for all powercuttype values."""
        # Act
        result = standardize_status(input_status)

        # Assert
        assert result == expected


class TestDatetimeNormalization:
    """Tests for normalizing datetime formats"""

    @pytest.mark.parametrize("input_datetime,expected", [
        ("2025-11-05T10:02:16", "2025-11-05T10:02:16"),
        ("2025-11-20T13:45:14.630701", "2025-11-20T13:45:14"),
        ("2025-11-05T10:02:16+00:00", "2025-11-05T10:02:16+00:00"),
        ("2025-11-20T13:45:14.123456+00:00", "2025-11-20T13:45:14+00:00"),
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

    def test_transform_data_uk_pow_single_postcode(self):
        """Test transformation with single postcode record."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'IP28 8',
            'outage_date': '2025-11-05T10:02:16',
            'status': 'Planned',
            'source_provider': 'UK Power Networks',
            'recording_time': '2025-11-20T13:45:14.630701'
        }]

        # Act
        result = transform_data_uk_pow(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['IP28 8']
        assert result[0]['status'] == 'planned'
        assert result[0]['outage_date'] == '2025-11-05T10:02:16'
        assert result[0]['recording_time'] == '2025-11-20T13:45:14'

    def test_transform_data_uk_pow_multiple_postcodes(self):
        """Test transformation with semicolon-separated postcodes."""
        # Arrange
        extracted_data = [{
            'affected_postcodes': 'IP28 8;IP29 4;IP30 0',
            'outage_date': '2025-11-05T10:02:16',
            'status': 'Unplanned',
            'source_provider': 'UK Power Networks',
            'recording_time': '2025-11-20T13:45:14.630701'
        }]

        # Act
        result = transform_data_uk_pow(extracted_data)

        # Assert
        assert len(result) == 1
        assert len(result[0]['affected_postcodes']) == 3
        assert result[0]['affected_postcodes'] == [
            'IP28 8', 'IP29 4', 'IP30 0']
        assert result[0]['status'] == 'unplanned'

    def test_transform_data_uk_pow_unknown_status(self):
        """Test transformation handles Restored/Multiple as unknown."""
        # Arrange
        extracted_data = [
            {
                'affected_postcodes': 'IP28 8',
                'outage_date': '2025-11-05T10:02:16',
                'status': 'Restored',
                'source_provider': 'UK Power Networks',
                'recording_time': '2025-11-20T13:45:14'
            },
            {
                'affected_postcodes': 'GU17 8',
                'outage_date': '2025-11-10T12:51:41',
                'status': 'Multiple',
                'source_provider': 'UK Power Networks',
                'recording_time': '2025-11-20T13:45:14'
            }
        ]

        # Act
        result = transform_data_uk_pow(extracted_data)

        # Assert
        assert len(result) == 2
        assert result[0]['status'] == 'unknown'
        assert result[1]['status'] == 'unknown'

    def test_transform_data_uk_pow_skips_invalid_records(self):
        """Test transformation skips records with no postcodes."""
        # Arrange
        extracted_data = [
            {
                'affected_postcodes': '',
                'outage_date': '2025-11-05T10:02:16',
                'status': 'Planned',
                'source_provider': 'UK Power Networks',
                'recording_time': '2025-11-20T13:45:14'
            },
            {
                'affected_postcodes': 'IP28 8',
                'outage_date': '2025-11-05T10:02:16',
                'status': 'Planned',
                'source_provider': 'UK Power Networks',
                'recording_time': '2025-11-20T13:45:14'
            }
        ]

        # Act
        result = transform_data_uk_pow(extracted_data)

        # Assert
        assert len(result) == 1
        assert result[0]['affected_postcodes'] == ['IP28 8']

    def test_transform_data_uk_pow_handles_empty_input(self):
        """Test transformation handles empty input list."""
        # Act
        result = transform_data_uk_pow([])

        # Assert
        assert result == []
