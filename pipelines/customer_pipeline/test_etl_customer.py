# pylint: skip-file
# pragma: no cover
import pytest
from unittest.mock import patch, Mock
from io import BytesIO
import pandas as pd
from botocore.exceptions import ClientError

from etl_customer import (
    format_name,
    format_email,
    format_postcode,
    transform,
    is_duplicate_customer,
    get_existing_customers,
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


class TestIsDuplicateCustomer:
    '''Tests for the is_duplicate_customer function in etl_customer module.'''

    def test_duplicate_customer_all_fields_match(self):
        '''Should return True when all customer fields match'''
        existing_df = pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }])
        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        assert is_duplicate_customer(existing_df, customer_data) == True

    def test_not_duplicate_different_first_name(self):
        '''Should return False when first_name differs'''
        existing_df = pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }])
        customer_data = {
            'first_name': 'Jane',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        assert is_duplicate_customer(existing_df, customer_data) == False

    def test_not_duplicate_different_last_name(self):
        '''Should return False when last_name differs'''
        existing_df = pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }])
        customer_data = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        assert is_duplicate_customer(existing_df, customer_data) == False

    def test_not_duplicate_different_email(self):
        '''Should return False when email differs'''
        existing_df = pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }])
        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'jane@example.com',
            'postcode': 'SW1A 1AA'
        }

        assert is_duplicate_customer(existing_df, customer_data) == False

    def test_not_duplicate_different_postcode(self):
        '''Should return False when postcode differs'''
        existing_df = pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }])
        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'M1 1AA'
        }

        assert is_duplicate_customer(existing_df, customer_data) == False


class TestGetExistingCustomers:
    '''Tests for the get_existing_customers function in etl_customer module.'''

    def test_get_existing_customers_nonexistent(self):
        '''Should return empty DataFrame and None ETag when file doesn't exist'''
        mock_s3_client = Mock()
        error_response = {'Error': {'Code': 'NoSuchKey'}}
        mock_s3_client.get_object.side_effect = ClientError(
            error_response, 'GetObject')

        result, etag = get_existing_customers(
            mock_s3_client, 'test-bucket', 'customers.parquet')

        assert result.empty
        assert len(result.columns) == 0
        assert etag is None

    def test_get_existing_customers_multiple_records(self):
        '''Should return DataFrame with multiple records and ETag'''
        expected_df = pd.DataFrame([
            {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com',
                'postcode': 'SW1A 1AA'
            },
            {
                'first_name': 'Jane',
                'last_name': 'Smith',
                'email': 'jane@example.com',
                'postcode': 'M1 1AA'
            }
        ])

        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        mock_s3_client = Mock()
        mock_s3_client.get_object.return_value = {
            'Body': buffer, 'ETag': '"abc123"'}
        result, etag = get_existing_customers(
            mock_s3_client, 'test-bucket', 'customers.parquet')

        pd.testing.assert_frame_equal(result, expected_df)
        assert len(result) == 2
        assert etag == 'abc123'


class TestLoad:
    '''Tests for the load function in etl_customer module.'''

    @patch.dict('os.environ', {'BUCKET_NAME': 'test-bucket'})
    @patch('etl_customer.get_existing_customers')
    @patch('etl_customer.boto3.client')
    def test_load_first_customer(self, mock_boto3_client, mock_get_existing):
        '''Should successfully load first customer'''
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        mock_get_existing.return_value = (pd.DataFrame(), None)

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }
        load(customer_data)

        assert mock_s3_client.put_object.called

    @patch.dict('os.environ', {'BUCKET_NAME': 'test-bucket'})
    @patch('etl_customer.get_existing_customers')
    @patch('etl_customer.is_duplicate_customer')
    @patch('etl_customer.boto3.client')
    def test_load_duplicate_raises_error(self, mock_boto3_client, mock_is_dup, mock_get_existing):
        '''Should raise ValueError for duplicate customer'''
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        mock_get_existing.return_value = (pd.DataFrame([{
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }]), 'etag123')
        mock_is_dup.return_value = True

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }

        with pytest.raises(ValueError) as excinfo:
            load(customer_data)
        assert str(excinfo.value) == "Customer data already exists in database"
        assert not mock_s3_client.put_object.called

    @patch.dict('os.environ', {'BUCKET_NAME': 'test-bucket'})
    @patch('etl_customer.get_existing_customers')
    @patch('etl_customer.is_duplicate_customer')
    @patch('etl_customer.boto3.client')
    def test_load_appends_new_customer(self, mock_boto3_client, mock_is_dup, mock_get_existing):
        '''Should append new customer to existing data'''
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        existing_df = pd.DataFrame([{
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com',
            'postcode': 'M1 1AA'
        }])
        mock_get_existing.return_value = (existing_df, 'etag456')
        mock_is_dup.return_value = False

        customer_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }
        load(customer_data)

        # Verify combined data saved to S3
        call_args = mock_s3_client.put_object.call_args
        body = call_args[1]['Body']
        df = pd.read_parquet(BytesIO(body))

        assert len(df) == 2
        assert df.iloc[0]['first_name'] == 'Jane'
        assert df.iloc[1]['first_name'] == 'John'


class TestLambdaHandler:
    '''Tests for the lambda_handler function in etl_customer module.'''

    @patch('etl_customer.load')
    @patch('etl_customer.transform')
    def test_lambda_handler_success(self, mock_transform, mock_load):
        '''Should return 200 status on successful ETL'''
        mock_event = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'postcode': 'SW1A 1AA'
        }
        mock_transform.return_value = mock_event
        mock_load.return_value = None

        response = lambda_handler(mock_event, None)

        assert response['statusCode'] == 200
        assert response['body'] == "Customer data processed successfully."
        mock_transform.assert_called_once_with(mock_event)
        mock_load.assert_called_once_with(mock_event)

    @patch('etl_customer.load')
    @patch('etl_customer.transform')
    def test_lambda_handler_transform_failure(self, mock_transform, mock_load):
        '''Should return 400 status when transform raises error'''
        mock_event = {'invalid': 'data'}
        mock_transform.side_effect = ValueError(
            "Missing required field: email.")

        response = lambda_handler(mock_event, None)

        assert response['statusCode'] == 400
        assert "Error:" in response['body']
        assert "Missing required field: email." in response['body']
        mock_load.assert_not_called()
