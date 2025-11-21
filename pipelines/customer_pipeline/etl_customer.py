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
import requests
import re
import os
import boto3
import pandas as pd
import io


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
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        formatted_postcode = data['result']['postcode']
        return formatted_postcode

    if response.status_code == 404:
        raise ValueError("Postcode is invalid according to postcodes.io API.")

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

    for field in formatters.keys():
        if not customer_data.get(field):
            raise ValueError(f"Missing required field: {field}.")

    for field, formatter in formatters.items():
        customer_data[field] = formatter(customer_data[field])

    return customer_data


def get_s3_client():
    '''
    Create and return an S3 client using boto3.

    Returns:
        boto3.client: S3 client object.
    '''
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    return s3_client


def load(customer_data: dict) -> None:
    '''
    Load customer data into S3 in parquet format.
    BytesIO: allows direct translation from pandas dataframe
        to S3 object, without needing to save a local file first.

    Args:
        customer_data (dict): Transformed customer data.
    '''
    s3_client = get_s3_client()
    bucket_name = os.getenv('BUCKET_NAME')
    s3_key = 'customers/customers.parquet'

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        existing_df = pd.read_parquet(io.BytesIO(response['Body'].read()))

    except s3_client.exceptions.NoSuchKey:  # 1st run, file doesn't exist yet
        existing_df = pd.DataFrame()

    new_df = pd.DataFrame([customer_data])

    if not existing_df.empty:
        is_duplicate = (
            (existing_df['first_name'] == customer_data['first_name']) &
            (existing_df['last_name'] == customer_data['last_name']) &
            (existing_df['email'] == customer_data['email']) &
            (existing_df['postcode'] == customer_data['postcode'])
        ).any()

        if is_duplicate:
            raise ValueError("Customer data already exists in database")

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    else:
        combined_df = new_df

    buffer = io.BytesIO()
    combined_df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3_client.put_object(Bucket=bucket_name, Key=s3_key,
                         Body=buffer.getvalue())


def lambda_handler(event, context) -> dict:
    '''
    Lambda function handler for customer ETL pipeline.

    Args:
        event (dict): Input JSON payload.
        context (object): Lambda context object.

    Returns:
        dict: Response object containing status and message.
    '''
    # Extract: event = customer data input

    # Transform
    customer_data = transform(event)

    ...
