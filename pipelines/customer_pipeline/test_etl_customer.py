# pylint: skip-file
# pragma: no cover
import pytest
from unittest.mock import patch, Mock
import psycopg2

from etl_customer import (
    format_name,
    format_email,
    format_postcode,
    transform,
    get_customer_id,
    load_customer,
    load,
    lambda_handler
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
        with pytest.raises(TypeError) as excinfo:
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
        with pytest.raises(TypeError) as excinfo:
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
                excinfo.value) == "Postcode is invalid according to regex pattern."

    def test_format_postcode_wrong_datatype(self):
        with pytest.raises(TypeError) as excinfo:
            format_postcode(12345)
        assert str(excinfo.value) == "Postcode must be a string datatype."


class TestTransform:
    '''Tests for the transform function in etl_customer module.'''

    @patch('etl_customer.format_name')
    @patch('etl_customer.format_email')
    @patch('etl_customer.format_postcode')
    def test_transform_valid(
            self,
            mock_format_postcode,
            mock_format_email,
            mock_format_name):
        event = {
            'first_name': ' john ',
            'last_name': ' DOE ',
            'email': 'abc@123.com',
            'postcode': 'EC1A 1BB'
        }
        mock_format_name.side_effect = lambda x: x.strip().title()
        mock_format_email.side_effect = lambda x: x.strip().lower()
        mock_format_postcode.side_effect = lambda x: x.strip().upper()
        transformed_event = transform(event)
        assert transformed_event['first_name'] == 'John'
        assert transformed_event['last_name'] == 'Doe'
        assert transformed_event['email'] == 'abc@123.com'
        assert transformed_event['postcode'] == 'EC1A 1BB'

    def test_transform_missing_field(self):
        event = {
            'first_name': 'John',
            'last_name': 'Doe',
            'postcode': 'EC1A 1BB'
        }
        with pytest.raises(ValueError) as excinfo:
            transform(event)
        assert str(excinfo.value) == "Missing required field: email."


class TestGetCustomerId:
    '''Tests for the get_customer_id function in etl_customer module.'''

    def test_get_customer_id_found(self):
        '''Should return customer_id when customer exists in database'''
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (42,)  # Found customer with id 42

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        result = get_customer_id(mock_conn, customer_data)
        assert result == 42
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_get_customer_id_not_found(self):
        '''Should return 0 when customer does not exist in database'''
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # No customer found

        customer_data = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com',
            'postcode': 'M1 1AA'
        }

        result = get_customer_id(mock_conn, customer_data)
        assert result == 0
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestLoadCustomer:
    '''Tests for the load_customer function in etl_customer module.'''

    @patch('etl_customer.get_customer_id')
    def test_load_customer_existing(self, mock_get_customer_id):
        '''Should return existing customer_id without inserting'''
        mock_get_customer_id.return_value = 42
        mock_conn = Mock()

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        result = load_customer(mock_conn, customer_data)
        assert result == 42
        mock_conn.cursor.assert_not_called()

    @patch('etl_customer.get_customer_id')
    def test_load_customer_new(self, mock_get_customer_id):
        '''Should insert new customer and return generated customer_id'''
        mock_get_customer_id.return_value = 0
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (99,)
        mock_conn.cursor.return_value = mock_cursor

        customer_data = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com',
            'postcode': 'M1 1AA'
        }

        result = load_customer(mock_conn, customer_data)
        assert result == 99
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestLoad:
    '''Tests for the load function in etl_customer module.'''

    @patch('etl_customer.load_customer')
    def test_load_success(self, mock_load_customer):
        '''Should successfully insert postcode subscription for new customer'''
        mock_load_customer.return_value = 42
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # No existing postcode subscription

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        load(mock_conn, customer_data)

        mock_load_customer.assert_called_once_with(mock_conn, customer_data)
        # Should execute twice: once for checking BRIDGE_subscribed_postcodes, once for INSERT
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()
        assert mock_cursor.close.call_count == 2

    @patch('etl_customer.load_customer')
    def test_load_duplicate_postcode_raises_error(self, mock_load_customer):
        '''Should raise ValueError when postcode subscription already exists'''
        mock_load_customer.return_value = 42
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (42,)  # Postcode subscription exists

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        with pytest.raises(ValueError) as excinfo:
            load(mock_conn, customer_data)
        assert str(excinfo.value) == "Postcode subscription already exists for postcode: SW1A 1AA"
        mock_load_customer.assert_called_once()


class TestLambdaHandler:
    '''Tests for the lambda_handler function in etl_customer module.'''

    @patch('etl_customer.connect_to_database')
    @patch('etl_customer.get_secrets')
    @patch('etl_customer.load')
    @patch('etl_customer.transform')
    def test_lambda_handler_success(self, mock_transform, mock_load,
                                    mock_get_secrets, mock_connect):
        '''Should return 200 status on successful ETL'''
        mock_event = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }
        mock_secrets = {'DB_HOST': 'localhost', 'DB_NAME': 'test'}
        mock_conn = Mock()
        mock_get_secrets.return_value = mock_secrets
        mock_connect.return_value = mock_conn
        mock_transform.return_value = mock_event
        mock_load.return_value = None

        response = lambda_handler(mock_event, None)

        assert response['statusCode'] == 200
        assert response['body'] == "Customer data processed successfully."
        mock_get_secrets.assert_called_once()
        mock_connect.assert_called_once_with(mock_secrets)
        mock_transform.assert_called_once_with(mock_event)
        mock_load.assert_called_once_with(mock_conn, mock_event)
        mock_conn.close.assert_called_once()

    @patch('etl_customer.connect_to_database')
    @patch('etl_customer.get_secrets')
    @patch('etl_customer.load')
    @patch('etl_customer.transform')
    def test_lambda_handler_transform_failure(self, mock_transform, mock_load,
                                              mock_get_secrets, mock_connect):
        '''Should return 400 status when transform raises error'''
        mock_event = {'invalid': 'data'}
        mock_secrets = {'DB_HOST': 'localhost', 'DB_NAME': 'test'}
        mock_conn = Mock()
        mock_get_secrets.return_value = mock_secrets
        mock_connect.return_value = mock_conn
        mock_transform.side_effect = ValueError("Missing required field: email.")

        response = lambda_handler(mock_event, None)

        assert response['statusCode'] == 400
        assert "Error:" in response['body']
        assert "Missing required field: email." in response['body']
        mock_load.assert_not_called()

    @patch('etl_customer.connect_to_database')
    @patch('etl_customer.get_secrets')
    def test_lambda_handler_database_connection_failure(self, mock_get_secrets,
                                                       mock_connect):
        '''Should return 500 status when database connection fails'''
        mock_event = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }
        mock_secrets = {'DB_HOST': 'localhost', 'DB_NAME': 'test'}
        mock_get_secrets.return_value = mock_secrets
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        response = lambda_handler(mock_event, None)

        assert response['statusCode'] == 500
        assert "Database error:" in response['body']
