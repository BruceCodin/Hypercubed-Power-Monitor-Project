# Assume the customer has already filled out the streamlit form with their
# Name, Email and a list of postcodes that they would like to be notified for...
# E.g. John Smith, john.smith@example.com, ["SW1A", "EC1A", "W1A"]

import logging
import os
import json

import psycopg2  # Standard library for PostgreSQL (RDS)
import boto3


SECRETS_ARN = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"


secrets = boto3.client('secretsmanager')
logger = logging.getLogger(__name__)


def get_secrets() -> dict:
    """Retrieve database credentials from AWS Secrets Manager.

    Returns:
        dict: Dictionary containing database credentials
    """

    client = boto3.client('secretsmanager')

    response = client.get_secret_value(
        SecretId=SECRETS_ARN
    )

    # Decrypts secret using the associated KMS key.
    secret = response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict


def load_secrets_to_env(secrets: dict):
    """Load database credentials from Secrets Manager into environment variables.

    Args:
        secrets (dict): Dictionary containing database credentials"""

    for key, value in secrets.items():
        os.environ[key] = str(value)


def connect_to_database() -> psycopg2.extensions.connection:
    """Connects to AWS Postgres database using Secrets Manager credentials.

    Returns:
        psycopg2 connection object
    """

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT")),
    )

    return conn


def submit_subscription_form(full_name, email, postcode_list, db_connection):
    """
    Handles the form submission with UPSERT logic:
    1. Splits name.
    2. UPSERTS customer (Update if email exists, Insert if new).
    3. CLEARS old subscriptions for this user.
    4. INSERTS new subscriptions.
    """

    # 1. PARSE NAME
    parts = full_name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    cursor = db_connection.cursor()

    try:
        # 2. UPSERT INTO DIM_customer
        # We use ON CONFLICT (email) to handle the duplicate email constraint.
        # DO UPDATE ensures we get the existing ID and update the name if it changed.
        upsert_customer_query = """
        INSERT INTO DIM_customer (first_name, last_name, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (email) 
        DO UPDATE SET 
            first_name = EXCLUDED.first_name, 
            last_name = EXCLUDED.last_name
        RETURNING customer_id;
        """

        cursor.execute(upsert_customer_query, (first_name, last_name, email))

        # This works for both new inserts and updates
        customer_id = cursor.fetchone()[0]
        print(f"Customer ID (Upserted): {customer_id}")

        # 3. CLEAR OLD SUBSCRIPTIONS
        # Before adding the new list, we wipe whatever they had before.
        # This effectively "replaces" their subscription list.
        delete_old_subs_query = """
        DELETE FROM BRIDGE_subscribed_postcodes 
        WHERE customer_id = %s;
        """
        cursor.execute(delete_old_subs_query, (customer_id,))
        print("Cleared previous subscriptions.")

        # 4. INSERT NEW SUBSCRIPTIONS
        insert_bridge_query = """
        INSERT INTO BRIDGE_subscribed_postcodes (customer_id, postcode)
        VALUES (%s, %s);
        """

        # The Loop
        for code in postcode_list:
            cursor.execute(insert_bridge_query, (customer_id, code))

        # Commit everything as a single atomic transaction
        db_connection.commit()
        print(f"Successfully updated to {len(postcode_list)} postcodes.")

    except Exception as e:
        db_connection.rollback()
        print(f"Error: {e}")

    finally:
        cursor.close()


# --- EXAMPLE USAGE ---
# This simulates the data coming from your Streamlit form
if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Mock data from Streamlit
    form_name = "M Muarij"
    form_email = "mohammadmuarijb@yahoo.co.uk"
    form_postcodes = ["SK5 8DY"]  # The list from st.multiselect

    # Load secrets and connect to DB
    logger.info("Loading secrets from Secrets Manager...")
    secrets = get_secrets()
    load_secrets_to_env(secrets)

    logger.info("Establishing database connection...")
    connection = connect_to_database()
    logger.info("Database connection established.")

    logger.info("Submitting subscription form...")
    submit_subscription_form(form_name, form_email, form_postcodes, connection)
    logger.info("Subscription form submitted.")

    connection.close()
