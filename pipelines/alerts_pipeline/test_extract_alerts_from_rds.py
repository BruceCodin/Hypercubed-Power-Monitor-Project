"""Unit tests for extract_alerts_from_rds module."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch, Mock, MagicMock
import json
from extract_alerts_from_rds import (
    get_secrets,
    load_secrets_to_env,
    connect_to_database,
    get_alerts_to_send
)


@patch('extract_alerts_from_rds.boto3.client')
def test_get_secrets_returns_dict(mock_boto_client):
    """Test that get_secrets returns a dictionary."""
    mock_client = Mock()
    mock_client.get_secret_value.return_value = {
        'SecretString': json.dumps({'DB_HOST': 'localhost', 'DB_PORT': '5432'})
    }
    mock_boto_client.return_value = mock_client

    result = get_secrets()
    assert isinstance(result, dict)
    assert 'DB_HOST' in result


@patch('extract_alerts_from_rds.boto3.client')
def test_get_secrets_parses_json_correctly(mock_boto_client):
    """Test that get_secrets correctly parses JSON from secret string."""
    mock_client = Mock()
    mock_client.get_secret_value.return_value = {
        'SecretString': json.dumps({
            'DB_HOST': 'test.rds.amazonaws.com',
            'DB_PORT': '5432',
            'DB_NAME': 'testdb'
        })
    }
    mock_boto_client.return_value = mock_client

    result = get_secrets()
    assert result['DB_HOST'] == 'test.rds.amazonaws.com'
    assert result['DB_PORT'] == '5432'


@patch('extract_alerts_from_rds.os.environ', {})
def test_load_secrets_to_env_sets_all_variables():
    """Test that all secrets are loaded into environment variables."""
    secrets = {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_USER': 'admin',
        'DB_PASSWORD': 'secret'
    }
    load_secrets_to_env(secrets)

    import os
    assert os.environ.get('DB_HOST') == 'localhost'
    assert os.environ.get('DB_PORT') == '5432'
    assert os.environ.get('DB_USER') == 'admin'
    assert os.environ.get('DB_PASSWORD') == 'secret'


@patch('extract_alerts_from_rds.os.environ', {})
def test_load_secrets_to_env_converts_integers_to_strings():
    """Test that integer values are converted to strings."""
    secrets = {'DB_PORT': 5432, 'DB_TIMEOUT': 30}
    load_secrets_to_env(secrets)

    import os
    assert os.environ.get('DB_PORT') == '5432'
    assert isinstance(os.environ.get('DB_PORT'), str)


@patch('extract_alerts_from_rds.psycopg2.connect')
@patch.dict('os.environ', {
    'DB_HOST': 'localhost',
    'DB_NAME': 'testdb',
    'DB_USER': 'user',
    'DB_PASSWORD': 'pass',
    'DB_PORT': '5432'
})
def test_connect_to_database_uses_environment_variables(mock_connect):
    """Test that database connection uses environment variables."""
    mock_conn = Mock()
    mock_connect.return_value = mock_conn

    connect_to_database()

    mock_connect.assert_called_once_with(
        host='localhost',
        database='testdb',
        user='user',
        password='pass',
        port=5432
    )


@patch('extract_alerts_from_rds.psycopg2.connect')
@patch.dict('os.environ', {
    'DB_HOST': 'localhost',
    'DB_NAME': 'testdb',
    'DB_USER': 'user',
    'DB_PASSWORD': 'pass',
    'DB_PORT': '5432'
})
def test_connect_to_database_returns_connection(mock_connect):
    """Test that connect_to_database returns a connection object."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    result = connect_to_database()
    assert result == mock_conn


def test_get_alerts_to_send_returns_list():
    """Test that get_alerts_to_send returns a list."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    result = get_alerts_to_send(mock_conn)
    assert isinstance(result, list)


def test_get_alerts_to_send_executes_query_with_group_by():
    """Test that SQL query includes GROUP BY clause."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_alerts_to_send(mock_conn)

    executed_query = mock_cursor.execute.call_args[0][0]
    assert 'GROUP BY' in executed_query
    assert 'STRING_AGG' in executed_query


def test_get_alerts_to_send_closes_cursor():
    """Test that cursor is closed after query execution."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_alerts_to_send(mock_conn)

    assert mock_cursor.close.called


def test_get_alerts_to_send_does_not_close_connection():
    """Test that connection is not closed (reusable for later)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_alerts_to_send(mock_conn)

    assert not mock_conn.close.called


def test_get_alerts_to_send_returns_correct_data_structure():
    """Test that returned data has expected tuple structure."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 'John', 'john@test.com', 101, '2025-01-15', 'SW1, SW2')
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_alerts_to_send(mock_conn)

    assert len(result) == 1
    assert len(result[0]) == 6
    assert result[0][0] == 1
    assert result[0][5] == 'SW1, SW2'


def test_get_alerts_to_send_includes_anti_spam_logic():
    """Test that query includes anti-spam LEFT JOIN."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_alerts_to_send(mock_conn)

    executed_query = mock_cursor.execute.call_args[0][0]
    assert 'FACT_notification_log' in executed_query
    assert 'LEFT JOIN' in executed_query
    assert 'IS NULL' in executed_query
