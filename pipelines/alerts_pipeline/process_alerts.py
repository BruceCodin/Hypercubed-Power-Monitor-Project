"""Functions for processing and sending alert notifications."""

import logging

import psycopg2
import boto3

logger = logging.getLogger(__name__)

ses_client = boto3.client('ses', region_name='eu-west-2')


def send_alert_email(first_name: str, email: str, outage_id: int,
                     outage_time: str, postcode_list: str) -> bool:
    """Send a single outage alert email via AWS SES.

    Args:
        first_name (str): Customer's first name
        email (str): Customer's email address
        outage_id (int): Outage ID
        outage_time (str): Time of outage
        postcode_list (str): Comma-separated list of affected postcodes

    Returns:
        bool: True if email sent successfully, False otherwise
    """

    # Compose email content
    subject = f"Power Outage Alert for {postcode_list}"
    body = f"Hi {first_name}\n\n" \
        f"There are power outages for the following " \
        f"postcodes you are subscribed to: {postcode_list}.\n\n" \
        f"Occured at: {outage_time}\n\n" \
        f"Regards,\nUK Power Monitor Team"

    try:
        ses_client.send_email(
            Source='mohammadmuarijb@yahoo.co.uk',
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        logger.info("Email sent to %s for Outage %d", email, outage_id)
        return True

    except Exception as e:
        logger.error("Failed to send email to %s: %s", email, e)
        return False


def log_notification(conn: psycopg2.extensions.connection,
                     customer_id: int, outage_id: int) -> bool:
    """Log a notification in the database to prevent duplicate alerts.

    Args:
        conn (psycopg2.extensions.connection): Database connection object
        customer_id (int): Customer ID
        outage_id (int): Outage ID

    Returns:
        bool: True if logged successfully, False otherwise
    """

    cursor = conn.cursor()

    log_insert = """
    INSERT INTO FACT_notification_log (customer_id, outage_id)
    VALUES (%s, %s)
    """

    try:
        cursor.execute(log_insert, (customer_id, outage_id))
        conn.commit()
        cursor.close()
        logger.info("Logged notification for customer %d, outage %d",
                    customer_id, outage_id)
        return True

    except Exception as e:
        logger.error("Failed to log notification for customer %d: %s",
                     customer_id, e)
        conn.rollback()
        cursor.close()
        return False


def process_alerts(conn: psycopg2.extensions.connection,
                   alerts: list) -> dict:
    """Process all pending alerts by sending emails and logging notifications.

    Args:
        conn (psycopg2.extensions.connection): Database connection object
        alerts (list): List of alert tuples from get_alerts_to_send()

    Returns:
        dict: Summary statistics containing 'sent', 'failed', and 'total'
    """

    stats = {'sent': 0, 'failed': 0, 'total': len(alerts)}

    for row in alerts:
        customer_id, first_name, email, outage_id, outage_time, postcode_list = row

        # Send the email
        email_sent = send_alert_email(
            first_name, email, outage_id, outage_time, postcode_list
        )

        # Only log if email was sent successfully
        if email_sent:
            logged = log_notification(conn, customer_id, outage_id)
            if logged:
                stats['sent'] += 1
            else:
                stats['failed'] += 1
                logger.warning(
                    "Email sent but failed to log for customer %d",
                    customer_id
                )
        else:
            stats['failed'] += 1
            logger.warning(
                "Skipping notification log for customer %d "
                "due to email failure", customer_id
            )

    logger.info("Processing complete: %d sent, %d failed out of %d total",
                stats['sent'], stats['failed'], stats['total'])

    return stats


if __name__ == "__main__":

    from extract_alerts_from_rds import (
        get_secrets,
        load_secrets_to_env,
        connect_to_database,
        get_alerts_to_send
    )

    # For local testing purposes

    secrets = get_secrets()
    load_secrets_to_env(secrets)
    connection = connect_to_database()
    alerts = get_alerts_to_send(connection)

    if alerts:
        stats = process_alerts(connection, alerts)
        print(f"Processing complete: {stats['sent']} sent, "
              f"{stats['failed']} failed out of {stats['total']} total")
    else:
        print("No pending alerts found.")
