def lambda_handler(event, context):
    """Main Lambda function handler."""

    # Load secrets from AWS Secrets Manager
    secrets = get_secrets()
    load_secrets_to_env(secrets)

    # Connect to the database
    conn = connect_to_database()
    cursor = conn.cursor()
