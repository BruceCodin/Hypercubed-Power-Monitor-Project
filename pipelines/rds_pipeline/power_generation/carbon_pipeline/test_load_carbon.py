"""Simple test suite for load_carbon module."""
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import psycopg2
from load_carbon import (
    load_settlement_data_to_db,
    load_carbon_data_to_db
)


class TestLoadCarbon(unittest.TestCase):
    """Test cases for load_carbon functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock(spec=psycopg2.extensions.connection)
        self.mock_cursor = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

    def test_load_settlement_data_no_connection(self):
        """Test load_settlement_data_to_db with no connection."""
        settlement_df = pd.DataFrame({
            'date': ['2025-01-01'],
            'settlement_period': [1]
        })

        result = load_settlement_data_to_db(None, settlement_df)
        self.assertIsNone(result)

    def test_load_settlement_data_success(self):
        """Test successful settlement data load."""
        settlement_df = pd.DataFrame({
            'date': ['2025-01-01', '2025-01-02'],
            'settlement_period': [1, 2]
        })

        self.mock_cursor.fetchall.return_value = None
        self.mock_connection.cursor.return_value = self.mock_cursor

        with patch('load_carbon.execute_values') as mock_execute:
            mock_execute.return_value = [(1,), (2,)]

            result = load_settlement_data_to_db(self.mock_connection, settlement_df)
            self.assertEqual(result, [1, 2])
            self.mock_connection.commit.assert_called_once()

    def test_load_settlement_data_integrity_error(self):
        """Test settlement data load with integrity error."""
        settlement_df = pd.DataFrame({
            'date': ['2025-01-01'],
            'settlement_period': [1]
        })

        self.mock_connection.cursor.return_value = self.mock_cursor

        with patch('load_carbon.execute_values') as mock_execute:
            mock_execute.side_effect = psycopg2.IntegrityError("Integrity error")

            result = load_settlement_data_to_db(self.mock_connection, settlement_df)
            self.assertIsNone(result)
            self.mock_connection.rollback.assert_called_once()

    def test_load_carbon_data_no_connection(self):
        """Test load_carbon_data_to_db with no connection."""
        carbon_df = pd.DataFrame({
            'intensity_forecast': [100],
            'intensity_actual': [95],
            'carbon_index': [50]
        })

        result = load_carbon_data_to_db(None, carbon_df)
        self.assertFalse(result)

    def test_load_carbon_data_success(self):
        """Test successful carbon data load."""
        carbon_df = pd.DataFrame({
            'intensity_forecast': [100, 110],
            'intensity_actual': [95, 105],
            'carbon_index': [50, 55]
        })

        self.mock_connection.cursor.return_value = self.mock_cursor

        with patch('load_carbon.load_settlement_data_to_db') as mock_settlement:
            mock_settlement.return_value = [1, 2]

            with patch('load_carbon.execute_values') as mock_execute:
                result = load_carbon_data_to_db(self.mock_connection, carbon_df)
                self.assertTrue(result)
                self.mock_connection.commit.assert_called_once()

    def test_load_carbon_data_settlement_failure(self):
        """Test carbon data load when settlement load fails."""
        carbon_df = pd.DataFrame({
            'intensity_forecast': [100],
            'intensity_actual': [95],
            'carbon_index': [50]
        })

        with patch('load_carbon.load_settlement_data_to_db') as mock_settlement:
            mock_settlement.return_value = None

            result = load_carbon_data_to_db(self.mock_connection, carbon_df)
            self.assertFalse(result)

    def test_load_carbon_data_integrity_error(self):
        """Test carbon data load with integrity error."""
        carbon_df = pd.DataFrame({
            'intensity_forecast': [100],
            'intensity_actual': [95],
            'carbon_index': [50]
        })

        with patch('load_carbon.load_settlement_data_to_db') as mock_settlement:
            mock_settlement.return_value = [1]

            with patch('load_carbon.execute_values') as mock_execute:
                mock_execute.side_effect = psycopg2.IntegrityError("Integrity error")

                result = load_carbon_data_to_db(self.mock_connection, carbon_df)
                self.assertFalse(result)
                self.mock_connection.rollback.assert_called_once()


if __name__ == '__main__':
    unittest.main()
