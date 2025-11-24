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
import logging
import os
import re
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError

# Postcode cache configuration
POSTCODE_CACHE = {}
CACHE_TTL_MINUTES = 60


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
        TypeError: If name is not a string.
        ValueError: If name is not a single nonempty word, 
            containing only alphabetic characters,
            or exceeds maximum length.
    '''
    if not isinstance(name, str):
        raise TypeError("Name must be a string datatype.")

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
        TypeError: If email is not a string.
        ValueError: If email is empty after stripping spaces,
            or does not match basic email format.
    '''
    if not isinstance(email, str):
        raise TypeError("Email must be a string datatype.")

    email = email.strip().lower()
    if not email:
        raise ValueError("Email must be a nonempty string.")

    if "@" not in email or "." not in email.split("@")[-1]:
        raise ValueError("Email must be a valid email address.")

    return email


def clear_expired_cache():
    '''
    Remove expired entries from postcode cache.
    '''
    now = datetime.now()
    expired_keys = [
        key for key, (_, timestamp) in POSTCODE_CACHE.items()
        if now - timestamp > timedelta(minutes=CACHE_TTL_MINUTES)
    ]
    for key in expired_keys:
        del POSTCODE_CACHE[key]


def format_postcode_with_regex(postcode: str) -> str:
    '''
    Helper: format and validate the customer's postcode using regex.
    Called when API fails or times out.
    Format with proper spacing: space before the last 3 characters (inward code)

    Args:
        postcode (str): The customer's postcode.

    Returns:
        str: Formatted postcode.

    Raises:
        ValueError: If postcode is invalid according to regex pattern.
    '''
    pattern = r'^([A-Z]{1,2}[0-9][A-Z0-9]?|[A-Z][0-9]{1,2})\s?([0-9][A-Z]{2})$'
    match = re.match(pattern, postcode)
    if not match:
        raise ValueError(
            "Postcode is invalid according to regex pattern.")

    postcode_cleaned = postcode.replace(' ', '')
    formatted_postcode = f"{postcode_cleaned[:-3]} {postcode_cleaned[-3:]}"
    return formatted_postcode


def format_postcode_with_api(postcode: str) -> str:
    '''
    Helper: format and validate the customer's postcode using postcodes.io API.
    Called when postcode is not in cache.
    If successful, caches the result.

    Args:
        postcode (str): The customer's postcode.

    Returns:
        str: Formatted postcode or empty string if API fails.

    Raises:
        ValueError: If postcode is invalid according to postcodes.io API.
    '''
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            formatted_postcode = data['result']['postcode']

            POSTCODE_CACHE[postcode] = (
                formatted_postcode, datetime.now())
            return formatted_postcode

        if response.status_code == 404:
            raise ValueError(
                "Postcode is invalid according to postcodes.io API.")
    except requests.exceptions.Timeout:
        return ""
    except requests.exceptions.RequestException:
        return ""
    return ""


def format_postcode(postcode: str) -> str:
    '''
    Format and validate the customer's postcode with caching.
    1. Check cache first (avoids API call for recently validated postcodes)
    2. Attempt postcodes.io API with short timeout (2 seconds instead of 5)
    3. If API fails or times out, use regex fallback immediately

    Args:
        postcode (str): The customer's postcode.

    Returns:
        str: Formatted postcode.

    Raises:
        TypeError: If postcode is not a string.
        ValueError: If postcode is invalid according to postcodes.io API,
            or is invalid according to regex pattern.
    '''
    if not isinstance(postcode, str):
        raise TypeError("Postcode must be a string datatype.")

    postcode_upper = postcode.strip().upper()

    clear_expired_cache()
    if postcode_upper in POSTCODE_CACHE:
        cached_result, _ = POSTCODE_CACHE[postcode_upper]
        return cached_result

    formatted_postcode = format_postcode_with_api(postcode_upper)
    if formatted_postcode:
        return formatted_postcode

    formatted_postcode = format_postcode_with_regex(postcode_upper)
    return formatted_postcode


def transform(event: dict) -> dict:
    '''
    Transform function to validate and format customer data fields.

    Args:
        event (dict): Input customer data.

    Returns:
        dict: Transformed customer data with formatted fields.

    Raises:
        ValueError: If any required field is missing from event.
        TypeError or ValueError: If any field fails validation (via helpers).
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


def get_existing_customers(
    s3_client: Any, bucket_name: str, s3_key: str
) -> tuple[pd.DataFrame, str | None]:
    '''
    Retrieve existing customer data from S3 with ETag for optimistic locking.
    BytesIO: allows direct translation from pandas dataframe
        to S3 object, without needing to save a local file first.

    Args:
        s3_client: Boto3 S3 client.
        bucket_name (str): S3 bucket name.
        s3_key (str): S3 object key.

    Returns:
        tuple: (DataFrame, ETag) - ETag is used for version control and preventing race conditions.
    '''
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        etag = response['ETag'].strip('"')  # Remove quotes from ETag
        return df, etag
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return pd.DataFrame(), None
        raise


def prepare_and_serialize_customer_data(existing_df: pd.DataFrame, customer_data: dict) -> bytes:
    '''
    Combine existing and new customer data, check for duplicates, and serialize to parquet bytes.
    BytesIO: allows direct translation from pandas dataframe to S3 object,
        without needing to save a local file first.

    Args:
        existing_df (pd.DataFrame): Existing customer data.
        customer_data (dict): New customer data to add.

    Returns:
        bytes: Serialized parquet data ready for S3 upload.

    Raises:
        ValueError: If customer already exists in database.
    '''
    new_df = pd.DataFrame([customer_data])

    if not existing_df.empty:
        if is_duplicate_customer(existing_df, customer_data):
            raise ValueError("Customer data already exists in database")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    buffer = BytesIO()
    combined_df.to_parquet(buffer, index=False, engine='pyarrow')
    return buffer.getvalue()


def upload_with_optimistic_locking(s3_client: Any, bucket_name: str, s3_key: str,
                                   parquet_bytes: bytes, etag: str | None) -> None:
    '''
    Upload parquet data to S3 with optimistic locking to prevent race conditions.

    Args:
        s3_client: Boto3 S3 client.
        bucket_name (str): S3 bucket name.
        s3_key (str): S3 object key.
        parquet_bytes (bytes): Serialized parquet data.
        etag (str | None): ETag for conditional write (None if file doesn't exist).

    Raises:
        ValueError: If write failed due to concurrent modification.
    '''
    try:
        if etag:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=parquet_bytes,
                IfMatch=etag
            )
        else:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=parquet_bytes,
                IfNoneMatch='*'
            )
    except ClientError as e:
        if e.response['Error']['Code'] == 'PreconditionFailed':
            raise ValueError(
                "Customer data was modified by another process. "
                "Please retry the operation."
            ) from e
        raise


def load(customer_data: dict) -> None:
    '''
    Load customer data into S3 with optimistic locking to prevent race conditions.

    Args:
        customer_data (dict): Transformed customer data.
    '''
    s3_client = boto3.client('s3')
    bucket_name = os.getenv('BUCKET_NAME')
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable is not set")
    s3_key = 'customers/customers.parquet'

    existing_df, etag = get_existing_customers(s3_client, bucket_name, s3_key)
    parquet_bytes = prepare_and_serialize_customer_data(
        existing_df, customer_data)
    upload_with_optimistic_locking(
        s3_client, bucket_name, s3_key, parquet_bytes, etag)


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
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        customer_data = transform(event)
        load(customer_data)
        logger.info("Customer data processed successfully.")
        return {
            'statusCode': 200,
            'body': "Customer data processed successfully."
        }

    except (ValueError, TypeError) as e:
        logger.warning("Validation error: %s", str(e))
        return {
            'statusCode': 400,
            'body': f"Error: {str(e)}"
        }
    except (ClientError, requests.exceptions.RequestException) as e:
        logger.error("Internal server error: %s", str(e))
        return {
            'statusCode': 500,
            'body': f"Internal server error: {str(e)}"
        }
