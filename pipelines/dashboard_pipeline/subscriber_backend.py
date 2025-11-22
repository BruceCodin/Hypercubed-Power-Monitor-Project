# Assume the customer has already filled out the streamlit form with their
# Name, Email and a list of postcodes that they would like to be notified for...
# E.g. John Smith, john.smith@example.com, ["SW1A", "EC1A", "W1A"]

import psycopg2  # Standard library for PostgreSQL (RDS)
import psycopg2  # Standard library for PostgreSQL (RDS)


def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="muarijmuarij",
        password="abc123",
        port=5432
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

    # Mock data from Streamlit
    form_name = "M Muarij"
    form_email = "mohammadmuarijb@yahoo.co.uk"
    form_postcodes = ["PL18 9JF", "PL18 9JL"]  # The list from st.multiselect

    # NOTE: In a real app, you would pass your actual RDS connection object here
    print("Establishing database connection...")
    connection = get_db_connection()
    print("Database connection established.")

    print("Submitting subscription form...")
    submit_subscription_form(form_name, form_email, form_postcodes, connection)
    print("Subscription form submitted.")

    connection.close()
