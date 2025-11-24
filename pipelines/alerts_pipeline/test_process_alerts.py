"""Unit tests for process_alerts module."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch, MagicMock
from process_alerts import (
    send_alert_email,
    log_notification,
    process_alerts
)


@patch('process_alerts.ses_client.send_email')
def test_send_alert_email_returns_true_on_success(mock_send_email):
    """Test that send_alert_email returns True when email sends successfully."""
    mock_send_email.return_value = {'MessageId': 'test-123'}

    result = send_alert_email('John', 'john@test.com',
                              101, '2025-01-15', 'SW1')
    assert result is True


@patch('process_alerts.ses_client.send_email')
def test_send_alert_email_returns_false_on_failure(mock_send_email):
    """Test that send_alert_email returns False when email fails."""
    mock_send_email.side_effect = Exception("SES Error")

    result = send_alert_email('John', 'john@test.com',
                              101, '2025-01-15', 'SW1')
    assert result is False


@patch('process_alerts.ses_client.send_email')
def test_send_alert_email_includes_customer_name(mock_send_email):
    """Test that email body includes customer's first name."""
    send_alert_email('Alice', 'alice@test.com', 102, '2025-01-15', 'N1')

    call_kwargs = mock_send_email.call_args[1]
    body = call_kwargs['Message']['Body']['Text']['Data']
    assert 'Hi Alice' in body


@patch('process_alerts.ses_client.send_email')
def test_send_alert_email_includes_postcodes_in_subject(mock_send_email):
    """Test that email subject includes postcode list."""
    send_alert_email('Bob', 'bob@test.com', 103, '2025-01-15', 'EC1, EC2')

    call_kwargs = mock_send_email.call_args[1]
    subject = call_kwargs['Message']['Subject']['Data']
    assert 'EC1, EC2' in subject


def test_log_notification_returns_true_on_success():
    """Test that log_notification returns True when logging succeeds."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = log_notification(mock_conn, 1, 101)
    assert result is True
    assert mock_conn.commit.called


def test_log_notification_returns_false_on_failure():
    """Test that log_notification returns False when logging fails."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("DB Error")
    mock_conn.cursor.return_value = mock_cursor

    result = log_notification(mock_conn, 1, 101)
    assert result is False
    assert mock_conn.rollback.called


def test_log_notification_closes_cursor():
    """Test that cursor is always closed after logging."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    log_notification(mock_conn, 1, 101)
    assert mock_cursor.close.called


def test_log_notification_inserts_correct_data():
    """Test that correct customer_id and outage_id are inserted."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    log_notification(mock_conn, 5, 205)

    executed_query = mock_cursor.execute.call_args[0]
    assert 'FACT_notification_log' in executed_query[0]
    assert executed_query[1] == (5, 205)


@patch('process_alerts.send_alert_email')
@patch('process_alerts.log_notification')
def test_process_alerts_returns_statistics(mock_log, mock_send):
    """Test that process_alerts returns correct statistics."""
    mock_send.return_value = True
    mock_log.return_value = True
    mock_conn = MagicMock()

    alerts = [
        (1, 'John', 'john@test.com', 101, '2025-01-15', 'SW1'),
        (2, 'Jane', 'jane@test.com', 102, '2025-01-15', 'N1')
    ]

    result = process_alerts(mock_conn, alerts)

    assert result['total'] == 2
    assert result['sent'] == 2
    assert result['failed'] == 0


@patch('process_alerts.send_alert_email')
@patch('process_alerts.log_notification')
def test_process_alerts_counts_failures_correctly(mock_log, mock_send):
    """Test that failed email attempts are counted correctly."""
    mock_send.side_effect = [True, False, True]
    mock_log.return_value = True
    mock_conn = MagicMock()

    alerts = [
        (1, 'John', 'john@test.com', 101, '2025-01-15', 'SW1'),
        (2, 'Jane', 'jane@test.com', 102, '2025-01-15', 'N1'),
        (3, 'Bob', 'bob@test.com', 103, '2025-01-15', 'EC1')
    ]

    result = process_alerts(mock_conn, alerts)

    assert result['sent'] == 2
    assert result['failed'] == 1


@patch('process_alerts.send_alert_email')
@patch('process_alerts.log_notification')
def test_process_alerts_only_logs_successful_emails(mock_log, mock_send):
    """Test that logging only happens for successfully sent emails."""
    mock_send.side_effect = [True, False]
    mock_log.return_value = True
    mock_conn = MagicMock()

    alerts = [
        (1, 'John', 'john@test.com', 101, '2025-01-15', 'SW1'),
        (2, 'Jane', 'jane@test.com', 102, '2025-01-15', 'N1')
    ]

    process_alerts(mock_conn, alerts)

    assert mock_log.call_count == 1
    assert mock_send.call_count == 2


@patch('process_alerts.send_alert_email')
@patch('process_alerts.log_notification')
def test_process_alerts_handles_empty_alert_list(mock_log, mock_send):
    """Test that process_alerts handles empty alert list correctly."""
    mock_conn = MagicMock()

    result = process_alerts(mock_conn, [])

    assert result['total'] == 0
    assert result['sent'] == 0
    assert result['failed'] == 0
    assert mock_send.call_count == 0
    assert mock_log.call_count == 0
