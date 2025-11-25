# pylint: skip-file
# pragma: no cover
import logging
from unittest.mock import patch, MagicMock
from transform_outages import (
    transform_postcode_manually,
    transform_postcode_list,
    transform_source_provider,
    transform_status,
    main_transform
)


class TestTransformPostcodeManually:
    '''Test class for transform_postcode_manually function.'''

    def test_transform_postcode_manually_valid(self):
        '''Test manual postcode transformation with valid postcodes.'''
        valid_cases = [
            ("BR8 7RE", "BR8 7RE"),
            ("br87re", "BR8 7RE"),
            ("SW1A1AA", "SW1A 1AA"),
        ]
        for input_code, expected in valid_cases:
            assert transform_postcode_manually(input_code) == expected

    def test_transform_postcode_manually_invalid(self):
        '''Test manual postcode transformation with invalid postcodes.'''
        invalid_cases = ["INVALID", 123, None, "BR8 7R", "BR8 7RE1"]
        for input_code in invalid_cases:
            assert transform_postcode_manually(input_code) == ""


class TestTransformPostcodeList:
    '''Test class for transform_postcode_list function.'''

    def setup_method(self):
        '''Setup logger for each test.'''
        self.logger = logging.getLogger(__name__)

    def test_transform_postcode_list_valid(self):
        '''Test postcode list transformation.'''
        input_postcodes = ["br87re", "invalid", "sw1a1aa", "ec1a1bb"]
        expected = ["BR8 7RE", "SW1A 1AA", "EC1A 1BB"]

        result = transform_postcode_list(input_postcodes, self.logger)
        assert result == expected

    def test_transform_postcode_list_all_invalid(self):
        '''Test postcode list transformation with all invalid postcodes.'''
        input_postcodes = ["invalid1", "invalid2", "invalid3"]

        result = transform_postcode_list(input_postcodes, self.logger)
        assert result == []

    def test_transform_postcode_list_mixed(self):
        '''Test postcode list transformation with mixed valid/invalid.'''
        input_postcodes = ["SW1A 1AA", "BADCODE", "EC1A 1BB"]
        expected = ["SW1A 1AA", "EC1A 1BB"]

        result = transform_postcode_list(input_postcodes, self.logger)
        assert result == expected


class TestTransformSourceProvider:
    '''Test class for transform_source_provider function.'''

    def test_transform_source_provider_valid(self):
        '''Test source provider transformation.'''
        valid_cases = [
            ("some provider", "Some Provider"),
            ("ANOTHER PROVIDER", "Another Provider"),
            ("Mixed Case Provider", "Mixed Case Provider"),
        ]
        for input_provider, expected in valid_cases:
            assert transform_source_provider(input_provider) == expected

    def test_transform_source_provider_invalid(self):
        '''Test source provider transformation with invalid inputs.'''
        invalid_cases = [123, None, "", []]
        for input_provider in invalid_cases:
            assert transform_source_provider(input_provider) == ""


class TestTransformStatus:
    '''Test class for transform_status function.'''

    def test_transform_status_valid(self):
        '''Test status transformation with valid inputs.'''
        valid_cases = [
            ("Planned", "planned"),
            ("unplanned", "unplanned"),
            (True, "planned"),
            (False, "unplanned"),
        ]
        for input_status, expected in valid_cases:
            assert transform_status(input_status) == expected

    def test_transform_status_invalid(self):
        '''Test status transformation with invalid inputs.'''
        invalid_cases = [123, [], {}]
        for input_status in invalid_cases:
            assert transform_status(input_status) == ""


class TestMainTransform:
    '''Test class for main_transform function.'''

    VALID_RECORDS = [
        (
            ["br87re", "sw1a1aa"],
            "2024-01-01T10:00:00",
            "some provider",
            True,
            "2024-01-01T09:00:00"
        ),
        (
            ["ec1a1bb"],
            "2024-01-02T11:00:00",
            "another provider",
            False,
            "2024-01-02T10:00:00"
        )
    ]
    EXPECTED_VALID = [
        {
            "affected_postcodes": ["BR8 7RE", "SW1A 1AA"],
            "outage_date": "2024-01-01T10:00:00",
            "source_provider": "Some Provider",
            "status": "planned",
            "recording_time": "2024-01-01T09:00:00"
        },
        {
            "affected_postcodes": ["EC1A 1BB"],
            "outage_date": "2024-01-02T11:00:00",
            "source_provider": "Another Provider",
            "status": "unplanned",
            "recording_time": "2024-01-02T10:00:00"
        }
    ]

    def setup_method(self):
        '''Setup logger for each test.'''
        self.logger = logging.getLogger(__name__)

    def _mock_datetime(self, iso_string):
        '''Create a mock datetime object.'''
        mock = MagicMock()
        mock.isoformat.return_value = iso_string
        return mock

    def _build_input_record(self, postcodes, outage_date, provider, status, recording_time):
        '''Build an input record with mock datetime objects.'''
        return {
            'affected_postcodes': postcodes,
            'outage_date': self._mock_datetime(outage_date),
            'source_provider': provider,
            'status': status,
            'recording_time': self._mock_datetime(recording_time)
        }

    @patch('transform_outages.transform_postcode_list')
    @patch('transform_outages.transform_source_provider')
    @patch('transform_outages.transform_status')
    def test_main_transform_valid(self, mock_status, mock_provider, mock_postcode):
        '''Test main transformation with valid input data.'''
        mock_postcode.side_effect = [["BR8 7RE", "SW1A 1AA"], ["EC1A 1BB"]]
        mock_provider.side_effect = ["Some Provider", "Another Provider"]
        mock_status.side_effect = ["planned", "unplanned"]

        input_data = [self._build_input_record(
            *record) for record in self.VALID_RECORDS]
        result = main_transform(input_data, self.logger)
        assert result == self.EXPECTED_VALID

    def test_main_transform_invalid(self):
        '''Test main transformation with invalid input data.'''
        input_data = [
            self._build_input_record(
                ["invalid"], "2024-01-01T10:00:00", "some provider", True, "2024-01-01T09:00:00"),
            {
                'affected_postcodes': ["BR8 7RE"],
                'source_provider': "another provider",
                'status': False,
                'recording_time': self._mock_datetime("2024-01-02T10:00:00")
            }
        ]
        result = main_transform(input_data, self.logger)
        assert result == []

    def test_main_transform_missing_field(self):
        '''Test main transformation with missing required field.'''
        input_data = [
            {
                'affected_postcodes': ["BR8 7RE"],
                'source_provider': "some provider",
                'status': True,
                'recording_time': self._mock_datetime("2024-01-01T09:00:00")
                # Missing outage_date
            }
        ]
        result = main_transform(input_data, self.logger)
        assert result == []

    def test_main_transform_partial_invalid_postcodes(self):
        '''Test main transformation where some postcodes are invalid.'''
        input_data = [
            self._build_input_record(
                ["BR8 7RE", "INVALID", "SW1A 1AA"],
                "2024-01-01T10:00:00",
                "some provider",
                True,
                "2024-01-01T09:00:00"
            )
        ]

        # The record should still be valid with the 2 valid postcodes
        expected = [
            {
                "affected_postcodes": ["BR8 7RE", "SW1A 1AA"],
                "outage_date": "2024-01-01T10:00:00",
                "source_provider": "Some Provider",
                "status": "planned",
                "recording_time": "2024-01-01T09:00:00"
            }
        ]

        result = main_transform(input_data, self.logger)
        assert result == expected
