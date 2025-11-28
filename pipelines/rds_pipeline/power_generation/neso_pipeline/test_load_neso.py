'''Simple test suite for load_neso module.'''
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import date
import psycopg2
from load_neso import (
    load_settlement_data_to_db,
    load_neso_demand_data_to_db
)
# pylint: skip-file
# pragma: no cover


class TestLoadSettlementDataToDb(unittest.TestCase):
    '''Tests for load_settlement_data_to_db function.'''

    def test_loads_settlement_data_successfully(self):
        '''Test that settlement data is loaded and IDs are returned.'''
        # Setup mock connection
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock the execute_values return value (list of tuples with IDs)
        mock_cursor.fetchall = Mock(return_value=[(1,), (2,), (3,)])

        # Prepare test data
        settlement_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            'settlement_period': [1, 2, 3]
        })

        # Patch execute_values to return settlement IDs
        with patch('load_neso.execute_values') as mock_execute:
            mock_execute.return_value = [(1,), (2,), (3,)]
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        # Verify results
        self.assertEqual(result, [1, 2, 3])
        mock_connection.commit.assert_called_once()

    def test_returns_none_when_no_connection(self):
        '''Test that function returns None when connection is None.'''
        settlement_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1]
        })

        result = load_settlement_data_to_db(None, settlement_df)
        self.assertIsNone(result)

    def test_handles_integrity_error(self):
        '''Test that integrity errors are handled gracefully.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        settlement_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1]
        })

        # Patch execute_values to raise IntegrityError
        with patch('load_neso.execute_values') as mock_execute:
            mock_execute.side_effect = psycopg2.IntegrityError("Constraint violation")
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        # Verify rollback was called and None returned
        self.assertIsNone(result)
        mock_connection.rollback.assert_called_once()

    def test_handles_key_error(self):
        '''Test that missing column errors are handled gracefully.'''
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # DataFrame missing required column
        settlement_df = pd.DataFrame({
            'wrong_column': [date(2023, 1, 1)]
        })

        result = load_settlement_data_to_db(mock_connection, settlement_df)

        # Verify rollback was called and None returned
        self.assertIsNone(result)
        mock_connection.rollback.assert_called_once()

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        settlement_df = pd.DataFrame({
            'settlement_date': [],
            'settlement_period': []
        })

        with patch('load_neso.execute_values') as mock_execute:
            mock_execute.return_value = []
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        self.assertEqual(result, [])
        mock_connection.commit.assert_called_once()

    def test_extracts_ids_from_tuples_correctly(self):
        '''Test that settlement IDs are extracted correctly from result tuples.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        settlement_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2]
        })

        # execute_values returns list of tuples like [(id,), (id,), ...]
        with patch('load_neso.execute_values') as mock_execute:
            mock_execute.return_value = [(42,), (43,)]
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        self.assertEqual(result, [42, 43])
        self.assertIsInstance(result, list)


class TestLoadNesoDemandDataToDb(unittest.TestCase):
    '''Tests for load_neso_demand_data_to_db function.'''

    def test_loads_demand_data_successfully(self):
        '''Test that demand data is loaded successfully.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2],
            'national_demand': [1000.5, 1100.3],
            'transmission_system_demand': [900.2, 950.1]
        })

        # Patch both execute_values calls
        with patch('load_neso.execute_values') as mock_execute, \
             patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1, 2]
            mock_execute.return_value = None

            result = load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

        self.assertTrue(result)
        mock_connection.commit.assert_called_once()

    def test_returns_false_when_no_connection(self):
        '''Test that function returns False when connection is None.'''
        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'national_demand': [1000.5],
            'transmission_system_demand': [900.2]
        })

        result = load_neso_demand_data_to_db(None, demand_df, 'recent_demand')
        self.assertFalse(result)

    def test_returns_false_when_settlement_load_fails(self):
        '''Test that function returns False when settlement loading fails.'''
        mock_connection = Mock()

        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'national_demand': [1000.5],
            'transmission_system_demand': [900.2]
        })

        with patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = None
            result = load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

        self.assertFalse(result)

    def test_handles_integrity_error_on_demand_insert(self):
        '''Test that integrity errors during demand insert are handled.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'national_demand': [1000.5],
            'transmission_system_demand': [900.2]
        })

        with patch('load_neso.execute_values') as mock_execute, \
             patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1]
            # execute_values is called once for demand insert, which fails
            mock_execute.side_effect = psycopg2.IntegrityError("Constraint violation")

            result = load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

        self.assertFalse(result)
        mock_connection.rollback.assert_called()

    def test_handles_key_error_on_demand_insert(self):
        '''Test that missing column errors are handled during demand insert.'''
        mock_connection = Mock()

        # DataFrame missing required column
        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'wrong_column': [1000.5]
        })

        with patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1]
            result = load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

        self.assertFalse(result)
        mock_connection.rollback.assert_called()

    def test_uses_correct_table_name(self):
        '''Test that the correct table name is used in the query.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'national_demand': [1000.5],
            'transmission_system_demand': [900.2]
        })

        with patch('load_neso.execute_values') as mock_execute, \
             patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1]
            mock_execute.return_value = None

            load_neso_demand_data_to_db(
                mock_connection, demand_df, 'historic_demand')

            # Verify execute_values was called with historic_demand table
            calls = mock_execute.call_args_list
            # Last call should be the demand data insert
            last_call_query = calls[-1][0][1]
            self.assertIn('historic_demand', last_call_query)

    def test_maps_settlement_ids_to_demand_data(self):
        '''Test that settlement IDs are correctly mapped to demand data.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        demand_df = pd.DataFrame({
            'settlement_date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2],
            'national_demand': [1000.5, 1100.3],
            'transmission_system_demand': [900.2, 950.1]
        })

        expected_settlement_ids = [10, 11]

        with patch('load_neso.execute_values') as mock_execute, \
             patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = expected_settlement_ids
            mock_execute.return_value = None

            load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

            # Get the data passed to execute_values for demand insert
            # First execute_values is for settlements (internal), second is for demand
            calls = mock_execute.call_args_list
            demand_data = calls[-1][0][2]  # Third argument is the data

            # Verify settlement IDs are in the demand data
            self.assertEqual(demand_data[0][0], 10)  # First settlement ID
            self.assertEqual(demand_data[1][0], 11)  # Second settlement ID

    def test_handles_empty_demand_dataframe(self):
        '''Test that empty demand DataFrame is handled.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        demand_df = pd.DataFrame({
            'settlement_date': [],
            'settlement_period': [],
            'national_demand': [],
            'transmission_system_demand': []
        })

        with patch('load_neso.execute_values') as mock_execute, \
             patch('load_neso.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = []
            mock_execute.return_value = None

            result = load_neso_demand_data_to_db(
                mock_connection, demand_df, 'recent_demand')

        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
