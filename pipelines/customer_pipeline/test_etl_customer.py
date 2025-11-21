import pytest
from unittest.mock import patch

from etl_customer import (
    format_name,
    format_email,
    format_postcode
)


class TestFormatName:
    '''Tests for the format_name function in etl_customer module.'''

    def test_format_name_valid(self):
        names = [
            ("John", "John"),
            ("dOE", "Doe"),
            ("  alice   ", "Alice"),
        ]
        for name, expected in names:
            formatted_name = format_name(name)
            assert formatted_name == expected

    def test_format_name_wrong_datatype(self):
        with pytest.raises(ValueError) as excinfo:
            format_name(123)
        assert str(excinfo.value) == "Name must be a string datatype."

    def test_format_name_non_alpha(self):
        names = ["", "123", "John123", "John Doe"]
        for name in names:
            with pytest.raises(ValueError) as excinfo:
                format_name(name)
            assert str(
                excinfo.value) == "Name must be a single nonempty word containing only alphabetic characters."

    def test_format_name_too_long(self):
        name = "A" * 36  # Note: assumes max_length is 35
        with pytest.raises(ValueError) as excinfo:
            format_name(name)
        assert str(excinfo.value) == "Name exceeds maximum length (35)."


class TestFormatEmail:
    '''Tests for the format_email function in etl_customer module.'''

    def test_format_email_valid(self):
        emails = [
            ("abc@123.com", "abc@123.com"),
            (" AB@XYZ.CO.UK  ", "ab@xyz.co.uk")
        ]
        for email, expected in emails:
            formatted_email = format_email(email)
            assert formatted_email == expected

    def test_format_email_wrong_datatype(self):
        with pytest.raises(ValueError) as excinfo:
            format_email(123)
        assert str(excinfo.value) == "Email must be a string datatype."

    def test_format_email_empty_string(self):
        with pytest.raises(ValueError) as excinfo:
            format_email("   ")
        assert str(excinfo.value) == "Email must be a nonempty string."

    def test_format_email_invalid(self):
        emails = ["plainaddress", "missingatsign.com", "missingdot@com"]
        for email in emails:
            with pytest.raises(ValueError) as excinfo:
                format_email(email)
            assert str(excinfo.value) == "Email must be a valid email address."


class TestFormatPostcode:
    '''Tests for the format_postcode function in etl_customer module.'''

    @patch('etl_customer.requests.get')
    def test_format_postcode_valid_api(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 200

        postcodes = [
            ("EC1A 1BB", "EC1A 1BB"),
            (" W1A0AX ", "W1A 0AX"),
            ("M1 1AE", "M1 1AE"),
            ("B33 8TH", "B33 8TH"),
            ("CR2 6XH", "CR2 6XH"),
            ("dn551pt", "DN55 1PT")
        ]

        for postcode, expected in postcodes:
            mock_response.json.return_value = {
                'result': {'postcode': expected}
            }
            formatted_postcode = format_postcode(postcode)
            assert formatted_postcode == expected

    @patch('etl_customer.requests.get')
    def test_format_postcode_invalid_api(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 404

        invalid_postcodes = ["INVALID1", "12345", "ABCDE"]

        for postcode in invalid_postcodes:
            with pytest.raises(ValueError) as excinfo:
                format_postcode(postcode)
            assert str(
                excinfo.value) == "Postcode is invalid according to postcodes.io API."

    @patch('etl_customer.requests.get')
    def test_format_postcode_valid_regex(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 500

        postcodes = [
            ("EC1A 1BB", "EC1A 1BB"),
            (" W1A0AX ", "W1A 0AX"),
            ("M1 1AE", "M1 1AE"),
            ("B33 8TH", "B33 8TH"),
            ("CR2 6XH", "CR2 6XH"),
            ("dn551pt", "DN55 1PT")
        ]

        for postcode, expected in postcodes:
            assert format_postcode(postcode) == expected

    @patch('etl_customer.requests.get')
    def test_format_postcode_invalid_regex(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 500

        invalid_postcodes = ["INVALID1", "12345",
                             "ABCDE", "EC1A1B", "W1A  0AX"]

        for postcode in invalid_postcodes:
            with pytest.raises(ValueError) as excinfo:
                format_postcode(postcode)
            assert str(
                excinfo.value) == "API postcodes.io inaccessible. Postcode is invalid according to regex pattern."

    def test_format_postcode_wrong_datatype(self):
        with pytest.raises(ValueError) as excinfo:
            format_postcode(12345)
        assert str(excinfo.value) == "Postcode must be a string datatype."
