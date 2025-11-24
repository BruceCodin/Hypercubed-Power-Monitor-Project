"""Unit tests for RDS extraction functions."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch, Mock, MagicMock
import json
import pandas as pd
from extract_from_rds import (
    get_secrets,
    load_secrets_to_env,
    connect_to_database,
    get_historical_power_generation_data,
    get_historical_carbon_data
)


@patch('extract_from_rds.boto3.client')
def test_get_secrets_success(mock_boto_client):
    """Test successful retrieval of secrets from AWS Secrets Manager."""
    mock_client = Mock()
    mock_client.get_secret_value.return_value = {
        'SecretString': json.dumps({'DB_HOST': 'localhost', 'DB_PORT': '5432'})
    }
    mock_boto_client.return_value = mock_client

    result = get_secrets()
    assert result == {'DB_HOST': 'localhost', 'DB_PORT': '5432'}
    mock_client.get_secret_value.assert_called_once()


@patch('extract_from_rds.boto3.client')
def test_get_secrets_returns_dict(mock_boto_client):
    """Test that get_secrets returns a dictionary."""
    mock_client = Mock()
    mock_client.get_secret_value.return_value = {
        'SecretString': json.dumps({'key': 'value'})
    }
    mock_boto_client.return_value = mock_client

    result = get_secrets()
    assert isinstance(result, dict)


@patch('extract_from_rds.os.environ', {})
def test_load_secrets_to_env_sets_variables():
    """Test loading secrets into environment variables."""
    secrets = {'DB_HOST': 'test.host', 'DB_PORT': '5432', 'DB_USER': 'admin'}
    load_secrets_to_env(secrets)

    import os
    assert os.environ.get('DB_HOST') == 'test.host'
    assert os.environ.get('DB_PORT') == '5432'
    assert os.environ.get('DB_USER') == 'admin'


@patch('extract_from_rds.os.environ', {})
def test_load_secrets_to_env_converts_to_string():
    """Test that numeric values are converted to strings."""
    secrets = {'DB_PORT': 5432}
    load_secrets_to_env(secrets)

    import os
    assert os.environ.get('DB_PORT') == '5432'
    assert isinstance(os.environ.get('DB_PORT'), str)


@patch('extract_from_rds.psycopg2.connect')
@patch.dict('os.environ', {
    'DB_HOST': 'localhost',
    'DB_NAME': 'testdb',
    'DB_USER': 'user',
    'DB_PASSWORD': 'pass',
    'DB_PORT': '5432'
})
def test_connect_to_database_success(mock_connect):
    """Test successful database connection."""
    mock_conn = Mock()
    mock_connect.return_value = mock_conn

    result = connect_to_database()
    assert result == mock_conn
    mock_connect.assert_called_once_with(
        host='localhost',
        database='testdb',
        user='user',
        password='pass',
        port=5432
    )


@patch('extract_from_rds.psycopg2.connect')
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
    assert result is not None


def test_get_historical_power_generation_data_returns_dataframe():
    """Test that function returns a pandas DataFrame."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 2, 1500.5, 100, 1, '2025-01-15')
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_power_generation_data(mock_conn)
    assert isinstance(result, pd.DataFrame)


def test_get_historical_power_generation_data_has_no_duplicate_columns():
    """Test that DataFrame has no duplicate columns."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 2, 1500.5, 100, 1, '2025-01-15')
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_power_generation_data(mock_conn)
    assert len(result.columns) == len(set(result.columns))


def test_get_historical_power_generation_data_executes_join_query():
    """Test that SQL query joins generation and settlements tables."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_historical_power_generation_data(mock_conn)

    executed_query = mock_cursor.execute.call_args[0][0]
    assert 'generation' in executed_query.lower()
    assert 'settlements' in executed_query.lower()
    assert 'join' in executed_query.lower()


def test_get_historical_power_generation_data_has_correct_columns():
    """Test that returned DataFrame has expected columns."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 2, 1500.5, 100, 1, '2025-01-15')
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_power_generation_data(mock_conn)
    expected_columns = ["generation_id", "fuel_type_id", "generation_mw",
                        "settlement_id", "settlement_period", "settlement_date"]

    for col in expected_columns:
        assert col in result.columns


def test_get_historical_carbon_data_returns_dataframe():
    """Test that function returns a pandas DataFrame."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 150, 145, 'moderate', 100, '2025-01-15', 1)
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_carbon_data(mock_conn)
    assert isinstance(result, pd.DataFrame)


def test_get_historical_carbon_data_has_no_duplicate_columns():
    """Test that DataFrame has no duplicate columns."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 150, 145, 'moderate', 100, '2025-01-15', 1)
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_carbon_data(mock_conn)
    assert len(result.columns) == len(set(result.columns))


def test_get_historical_carbon_data_executes_join_query():
    """Test that SQL query joins carbon_intensity and settlements tables."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    get_historical_carbon_data(mock_conn)

    executed_query = mock_cursor.execute.call_args[0][0]
    assert 'carbon_intensity' in executed_query.lower()
    assert 'settlements' in executed_query.lower()
    assert 'join' in executed_query.lower()


def test_get_historical_carbon_data_has_correct_columns():
    """Test that returned DataFrame has expected columns."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, 150, 145, 'moderate', 100, '2025-01-15', 1)
    ]
    mock_conn.cursor.return_value = mock_cursor

    result = get_historical_carbon_data(mock_conn)
    expected_columns = ["intensity_id", "intensity_forecast",
                        "intensity_actual", "intensity_index",
                        "settlement_id", "settlement_date",
                        "settlement_period"]

    for col in expected_columns:
        assert col in result.columns
