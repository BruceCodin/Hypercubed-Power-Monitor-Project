"""Debug script to test alerts lambda query and email logic."""

import psycopg2
import boto3

# Initialize AWS SES client
ses_client = boto3.client('ses', region_name='eu-west-2')


def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="muarijmuarij",
        password="abc123",
        port=5432
    )
    return conn


def test_query():
    """Test the query to see what it returns."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        c.customer_id,
        c.first_name,
        c.email,
        o.outage_id,
        o.outage_date,
        STRING_AGG(DISTINCT bsp.postcode, ', ') as postcodes
    FROM FACT_outage o
    JOIN BRIDGE_affected_postcodes bap ON o.outage_id = bap.outage_id
    JOIN BRIDGE_subscribed_postcodes bsp ON bap.postcode_affected = bsp.postcode
    JOIN DIM_customer c ON bsp.customer_id = c.customer_id
    -- The Anti-Spam Join:
    LEFT JOIN FACT_notification_log log
        ON c.customer_id = log.customer_id
        AND o.outage_id = log.outage_id
    WHERE
        log.notification_id IS NULL -- Only if we haven't emailed them yet
    GROUP BY
        c.customer_id, c.first_name, c.email, o.outage_id, o.outage_date;
    """

    print("=" * 80)
    print("TESTING ALERTS QUERY")
    print("=" * 80)

    cursor.execute(query)
    alerts_to_send = cursor.fetchall()

    print(f"\nFound {len(alerts_to_send)} pending notifications.\n")

    if len(alerts_to_send) == 0:
        print("No pending notifications found.")
        print("\nPossible reasons:")
        print("  1. No active outages")
        print("  2. No customers subscribed to affected postcodes")
        print("  3. All notifications already sent (check FACT_notification_log)")
    else:
        print("Query Results:")
        print("-" * 80)
        for i, row in enumerate(alerts_to_send, 1):
            customer_id, first_name, email, outage_id, outage_date, postcode_list = row
            print(f"\nRow {i}:")
            print(f"  Customer ID: {customer_id}")
            print(f"  Name: {first_name}")
            print(f"  Email: {email}")
            print(f"  Outage ID: {outage_id}")
            print(f"  Outage Date: {outage_date}")
            print(f"  Postcodes: {postcode_list}")
            print(
                f"  → This will send 1 email to {email} mentioning: {postcode_list}")

        print("\n" + "=" * 80)
        print(f"TOTAL EMAILS TO SEND: {len(alerts_to_send)}")
        print("=" * 80)

        if len(alerts_to_send) > 1:
            print("\nWhy multiple emails?")
            print("  Each row = 1 email")
            print("  If same customer appears multiple times, it means:")
            print("    - Multiple different outages (different outage_id)")
            print("    - Each outage gets its own email (this is correct!)")

    cursor.close()
    conn.close()


def test_send_emails():
    """Actually send the emails (like the real lambda does)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        c.customer_id,
        c.first_name,
        c.email,
        o.outage_id,
        o.outage_date,
        STRING_AGG(DISTINCT bsp.postcode, ', ') as postcodes
    FROM FACT_outage o
    JOIN BRIDGE_affected_postcodes bap ON o.outage_id = bap.outage_id
    JOIN BRIDGE_subscribed_postcodes bsp ON bap.postcode_affected = bsp.postcode
    JOIN DIM_customer c ON bsp.customer_id = c.customer_id
    LEFT JOIN FACT_notification_log log
        ON c.customer_id = log.customer_id
        AND o.outage_id = log.outage_id
    WHERE
        log.notification_id IS NULL
    GROUP BY
        c.customer_id, c.first_name, c.email, o.outage_id, o.outage_date;
    """

    cursor.execute(query)
    alerts_to_send = cursor.fetchall()

    print("\n" + "=" * 80)
    print("SENDING EMAILS")
    print("=" * 80)

    for row in alerts_to_send:
        customer_id, first_name, email, outage_id, outage_date, postcode_list = row

        subject = f"Power Outage Alert for {postcode_list}"
        body = f"Hi {first_name},\n\nThere is a power outage affecting the following postcodes: {postcode_list}.\nReported at {outage_date}. We are working on it."

        print(
            f"\n📧 Sending email {alerts_to_send.index(row) + 1}/{len(alerts_to_send)}:")
        print(f"   To: {email}")
        print(f"   Subject: {subject}")
        print(f"   Body preview: {body[:80]}...")

        try:
            response = ses_client.send_email(
                Source='mohammadmuarijb@yahoo.co.uk',
                Destination={'ToAddresses': [email]},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {'Text': {'Data': body}}
                }
            )
            print(
                f"   ✅ Email sent successfully! MessageId: {response['MessageId']}")

            # Log the notification
            log_insert = """
            INSERT INTO FACT_notification_log (customer_id, outage_id)
            VALUES (%s, %s)
            """
            cursor.execute(log_insert, (customer_id, outage_id))
            conn.commit()
            print(f"   ✅ Logged to FACT_notification_log")

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            conn.rollback()

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    print("EMAIL SENDING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    print("\n🔍 OPTION 1: Test Query (see what will be sent)")
    print("📧 OPTION 2: Actually Send Emails\n")

    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        test_query()
    elif choice == "2":
        confirm = input(
            "⚠️  This will send real emails! Are you sure? (yes/no): ").strip().lower()
        if confirm == "yes":
            test_send_emails()
        else:
            print("Cancelled.")
    else:
        print("Invalid choice. Run again and enter 1 or 2.")
