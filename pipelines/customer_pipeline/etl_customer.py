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
import json
from datetime import datetime, timedelta
import boto3
import psycopg2
import requests

# Postcode cache configuration
POSTCODE_CACHE = {}
CACHE_TTL_MINUTES = 60


def get_and_load_secrets() -> None:
def get_secrets() -> dict:
    """
    Retrieve database credentials from AWS Secrets Manager.

    Returns:
        dict: Secrets dictionary containing DB credentials.
    """
    secrets_arn = os.getenv("SECRETS_ARN")
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secrets_arn)
    secret = response['SecretString']
    secret_dict = json.loads(secret)
    return secret_dict


def connect_to_database(secrets: dict) -> psycopg2.extensions.connection:
    """
    Connects to AWS Postgres database using Secrets Manager credentials.

    Args:
        secrets (dict): Database credentials.

    Returns:
        psycopg2 connection object
    """
    conn = psycopg2.connect(
        host=secrets["DB_HOST"],
        database=secrets["DB_NAME"],
        user=secrets["DB_USER"],
        password=secrets["DB_PASSWORD"],
        port=int(secrets["DB_PORT"]),
    )
    return conn


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
        response = requests.get(url, timeout=1)
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


def get_customer_id(conn: psycopg2.extensions.connection, customer_data: dict) -> int:
    '''
    Check if customer already exists in the database, 
    return customer_id if found.

    Args:
        conn: psycopg2 connection object
        customer_data (dict): New customer data to check.

    Returns:
        int: customer_id if found, else 0.
    '''
    cursor = conn.cursor()
    cursor.execute('''
        SELECT customer_id FROM DIM_customer
        WHERE first_name = %s AND last_name = %s AND email = %s
        LIMIT 1
    ''', (
        customer_data['first_name'],
        customer_data['last_name'],
        customer_data['email']
    ))
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]
    not_found = 0
    return not_found


def load_customer(conn: psycopg2.extensions.connection, customer_data: dict) -> int:
    '''
    Load customer data into DIM_customer table if not already present.
    Get generated customer_id with lastrowid.

    Args:
        conn: psycopg2 connection object
        customer_data (dict): Transformed customer data.

    Returns:
        int: customer_id of the inserted or existing customer.
    '''
    customer_id = get_customer_id(conn, customer_data)
    if not customer_id:
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO DIM_customer (first_name, last_name, email)
        VALUES (%s, %s, %s)
        RETURNING customer_id
        ''', (
            customer_data['first_name'],
            customer_data['last_name'],
            customer_data['email']
        ))
        customer_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
    return customer_id


def load(conn: psycopg2.extensions.connection, customer_data: dict) -> None:
    '''
    Load customer data into RDS database with duplicate check:
        DIM_customer table handled in load_customer function.
        Check for existing postcode subscription in 
        BRIDGE_subscribed_postcodes table.

    Args:
        conn: psycopg2 connection object
        customer_data (dict): Transformed customer data.

    Raises:
        ValueError: If a subscription for the given postcode already exists in the database
            (regardless of which customer is associated with it).
    '''
    customer_id = load_customer(conn, customer_data)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT customer_id FROM BRIDGE_subscribed_postcodes
        WHERE postcode = %s
        LIMIT 1
    ''', (customer_data['postcode'],))
    result = cursor.fetchone()
    cursor.close()
    if result:
        raise ValueError(f"Postcode subscription already exists for postcode: {customer_data['postcode']}")

    cursor.execute('''
        INSERT INTO BRIDGE_subscribed_postcodes (customer_id, postcode)
        VALUES (%s, %s)
    ''', (
        customer_id,
        customer_data['postcode']
    ))
    conn.commit()
    cursor.close()


def main(logger: logging.Logger, event: dict) -> None:
    '''
    Main function for customer ETL pipeline 
    (within try block of lambda_handler).

    Args:
        logger (logging.Logger): Logger object for logging.
        event (dict): Input JSON payload.
    '''
    logger.info("Fetching secrets from Secrets Manager")
    get_and_load_secrets()
    logger.info("Secrets loaded to environment variables")

    logger.info("Connecting to database at %s:%s",
                os.getenv('DB_HOST'), os.getenv('DB_PORT'))
    db_conn = connect_to_database()
    logger.info("Database connection successful")
    try:
        customer_data = transform(event)
        load(db_conn, customer_data)
        logger.info("Customer data processed successfully.")
    finally:
        db_conn.close()
def lambda_handler(event, _context) -> dict:
    '''
    Lambda function handler for customer ETL pipeline.
    1. Extract: receive customer data from JSON payload.
    2. Transform: validate and format data fields.
    3. Load: move data into the customer database (RDS).

    Args:
        event (dict): Input JSON payload.
        _context (object): Lambda context object.

    Returns:
        dict: Response object containing status and message.
    '''
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        main(logger, event)
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
    except psycopg2.Error as e:
        logger.error("Database error: %s", str(e))
        return {
            'statusCode': 500,
            'body': f"Database error: {str(e)}"
        }
    except requests.exceptions.RequestException as e:
        logger.error("Request error: %s", str(e))
        return {
            'statusCode': 500,
            'body': f"Request error: {str(e)}"
        }
