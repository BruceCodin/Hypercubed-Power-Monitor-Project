'''
Customer ETL Pipeline
This module contains the ETL process for customer data
to be used on a lambda function.

Input: JSON payload upon trigger by streamlit dashboard
    {
        "first_name": str,
        "last_name": str,
        "email": str,
        "postcode": str
    }

Process:
    1. Extract: receiving data from JSON payload.
    2. Transform: validate and format data fields.
    3. Load: move data into the customer database.

Output:
    {
        "status": int(200 for success, 400 for failure),
        "message": str(success or error description (field, type of error etc.))
    }
'''
import os
import re
from io import BytesIO
from typing import Any

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError


def format_name(name: str) -> str:
    '''
    Format the customer's first or last name to title case and strip extra spaces.
    Additional check: length of name (not currently set in schema)
    Note: isalpha checks for alphabetic characters only:
        this includes no spaces and not empty.
    Args:
        name (str): The customer's name.

    Returns:
        str: Formatted name.

    Raises:
        ValueError: If name is not a string, 
            contains non-alphabetic characters,
            or exceeds maximum length.
    '''
    if not isinstance(name, str):
        raise ValueError("Name must be a string datatype.")

    name = name.strip().title()

    if not name.isalpha():
        raise ValueError(
            "Name must be a single nonempty word containing only alphabetic characters.")

    max_length = 35
    if len(name) > max_length:
        raise ValueError(f"Name exceeds maximum length ({max_length}).")

    return name


def format_email(email: str) -> str:
    '''
    Format and validate the customer's email address.

    Args:
        email (str): The customer's email address.

    Returns:
        str: Formatted email address.

    Raises:
        ValueError: If email is not a string,
            is empty after stripping spaces,
            or does not match basic email format.
    '''
    if not isinstance(email, str):
        raise ValueError("Email must be a string datatype.")

    email = email.strip().lower()
    if not email:
        raise ValueError("Email must be a nonempty string.")

    if "@" not in email or "." not in email.split("@")[-1]:
        raise ValueError("Email must be a valid email address.")

    return email


def format_postcode(postcode: str) -> str:
    '''
    Format and validate the customer's postcode.
    1. Attempt this first with postcodes.io API. Docs:
    https://postcodes.io/docs/postcode/lookup/
    2. If API fails, use regex pattern matching according to this format:
    https://ideal-postcodes.co.uk/guides/uk-postcode-format

    Args:
        postcode (str): The customer's postcode.

    Returns:
        str: Formatted postcode.

    Raises:
        ValueError: If postcode is not a string,
            is invalid according to postcodes.io API,
            or is invalid according to regex pattern.
    '''

    if not isinstance(postcode, str):
        raise ValueError("Postcode must be a string datatype.")

    url = f"https://api.postcodes.io/postcodes/{postcode}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            formatted_postcode = data['result']['postcode']
            return formatted_postcode

        if response.status_code == 404:
            raise ValueError(
                "Postcode is invalid according to postcodes.io API.")
    except requests.exceptions.RequestException:
        pass  # log here

    postcode = postcode.strip().upper()

    pattern = r'^([A-Z]{1,2}[0-9][A-Z0-9]?|[A-Z][0-9]{1,2})\s?([0-9][A-Z]{2})$'
    match = re.match(pattern, postcode)
    if not match:
        raise ValueError(
            "API postcodes.io inaccessible. Postcode is invalid according to regex pattern.")

    # Format with proper spacing: space before the last 3 characters (inward code)
    postcode_cleaned = postcode.replace(' ', '')
    formatted_postcode = f"{postcode_cleaned[:-3]} {postcode_cleaned[-3:]}"
    return formatted_postcode


def transform(event: dict) -> dict:
    '''
    Transform function to validate and format customer data fields.

    Args:
        event (dict): Input customer data.

    Returns:
        dict: Transformed customer data with formatted fields.

    Raises:
        ValueError: If any required field is missing from event,
        or if any field fails validation (via helpers).
    '''
    customer_data = event.copy()
    formatters = {
        'first_name': format_name,
        'last_name': format_name,
        'email': format_email,
        'postcode': format_postcode
    }

    for field in formatters:
        if field not in customer_data:
            raise ValueError(f"Missing required field: {field}.")

    for field, formatter in formatters.items():
        customer_data[field] = formatter(customer_data[field])

    return customer_data


def get_s3_client() -> Any:
    '''
    Create and return an S3 client using boto3.

    Returns:
        Any: S3 client object.
    '''
    s3_client = boto3.client('s3')
    return s3_client


def is_duplicate_customer(existing_df: pd.DataFrame, customer_data: dict) -> bool:
    '''
    Check if customer already exists in the dataframe.

    Args:
        existing_df (pd.DataFrame): Existing customer data.
        customer_data (dict): New customer data to check.

    Returns:
        bool: True if customer is a duplicate, False otherwise.
    '''
    return (
        (existing_df['first_name'] == customer_data['first_name']) &
        (existing_df['last_name'] == customer_data['last_name']) &
        (existing_df['email'] == customer_data['email']) &
        (existing_df['postcode'] == customer_data['postcode'])
    ).any()


def get_existing_customers(s3_client: Any, bucket_name: str, s3_key: str) -> pd.DataFrame:
    '''
    Retrieve existing customer data from S3, or return empty DataFrame if file doesn't exist.
    BytesIO: allows direct translation from pandas dataframe
        to S3 object, without needing to save a local file first.

    Args:
        s3_client: Boto3 S3 client.
        bucket_name (str): S3 bucket name.
        s3_key (str): S3 object key.

    Returns:
        pd.DataFrame: Existing customer data or empty DataFrame.
    '''
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return pd.DataFrame()
        raise


def load(customer_data: dict) -> None:
    '''
    Load customer data into S3 in parquet format if unique.
    BytesIO: allows direct translation from pandas dataframe
        to S3 object, without needing to save a local file first.

    Args:
        customer_data (dict): Transformed customer data.
    '''
    s3_client = get_s3_client()
    bucket_name = os.getenv('BUCKET_NAME')
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable is not set")
    s3_key = 'customers/customers.parquet'

    existing_df = get_existing_customers(s3_client, bucket_name, s3_key)
    new_df = pd.DataFrame([customer_data])

    if not existing_df.empty:
        if is_duplicate_customer(existing_df, customer_data):
            raise ValueError("Customer data already exists in database")

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    else:
        combined_df = new_df

    buffer = BytesIO()
    combined_df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3_client.put_object(Bucket=bucket_name, Key=s3_key,
                         Body=buffer.getvalue())


def lambda_handler(event, _context) -> dict:
    '''
    Lambda function handler for customer ETL pipeline.
    1. Extract: receive customer data from JSON payload.
    2. Transform: validate and format data fields.
    3. Load: move data into the customer database (S3).

    Args:
        event (dict): Input JSON payload.
        _context (object): Lambda context object.

    Returns:
        dict: Response object containing status and message.
    '''
    try:
        customer_data = transform(event)
        load(customer_data)

    except ValueError as e:
        return {
            'statusCode': 400,
            'body': f"Error: {str(e)}"
        }
    except (ClientError, requests.exceptions.RequestException) as e:
        return {
            'statusCode': 500,
            'body': f"Internal server error: {str(e)}"
        }

    return {
        'statusCode': 200,
        'body': "Customer data processed successfully."
    }
