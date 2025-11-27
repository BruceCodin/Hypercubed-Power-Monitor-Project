'''Simple test suite for load_elexon module.'''
import unittest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import date
import psycopg2
from load_elexon import (
    load_settlement_data_to_db,
    load_price_data_to_db,
    load_fuel_types_to_db,
    load_generation_data_to_db
)
# pylint: skip-file
# pragma: no cover


class TestLoadSettlementDataToDb(unittest.TestCase):
    '''Tests for load_settlement_data_to_db function.'''

    def test_loads_settlement_data_successfully(self):
        '''Test that settlement data is loaded and IDs are returned.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        settlement_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2]
        })

        # Mock execute_values to return settlement data with IDs
        with patch('load_elexon.execute_values') as mock_execute:
            mock_execute.return_value = [
                (1, date(2023, 1, 1), 1),
                (2, date(2023, 1, 2), 2)
            ]
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        self.assertEqual(result, [1, 2])
        mock_connection.commit.assert_called_once()

    def test_returns_none_when_no_connection(self):
        '''Test that function returns None when connection is None.'''
        settlement_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
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
            'date': [date(2023, 1, 1)],
            'settlement_period': [1]
        })

        with patch('load_elexon.execute_values') as mock_execute:
            mock_execute.side_effect = psycopg2.IntegrityError("Constraint violation")
            result = load_settlement_data_to_db(mock_connection, settlement_df)

        self.assertIsNone(result)
        mock_connection.rollback.assert_called_once()

    def test_handles_key_error(self):
        '''Test that missing column errors are handled gracefully.'''
        mock_connection = Mock()

        # DataFrame missing required column
        settlement_df = pd.DataFrame({
            'wrong_column': [date(2023, 1, 1)]
        })

        result = load_settlement_data_to_db(mock_connection, settlement_df)
        self.assertIsNone(result)
        mock_connection.rollback.assert_called_once()

    def test_deduplicates_settlements(self):
        '''Test that duplicate settlements are deduplicated.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        settlement_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 1, 2]
        })

        with patch('load_elexon.execute_values') as mock_execute:
            mock_execute.return_value = [
                (1, date(2023, 1, 1), 1),
                (2, date(2023, 1, 2), 2)
            ]
            load_settlement_data_to_db(mock_connection, settlement_df)

        # Check that execute_values was called with 2 unique settlements
        call_args = mock_execute.call_args
        unique_settlements = call_args[0][2]
        self.assertEqual(len(unique_settlements), 2)


class TestLoadPriceDataToDb(unittest.TestCase):
    '''Tests for load_price_data_to_db function.'''

    def test_loads_price_data_successfully(self):
        '''Test that price data is loaded successfully.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        price_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2],
            'system_sell_price': [50.5, 60.3]
        })

        with patch('load_elexon.execute_values') as mock_execute, \
             patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1, 2]
            mock_execute.return_value = None

            result = load_price_data_to_db(mock_connection, price_df)

        self.assertTrue(result)
        mock_connection.commit.assert_called_once()

    def test_returns_false_when_no_connection(self):
        '''Test that function returns False when connection is None.'''
        price_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'system_sell_price': [50.5]
        })
        result = load_price_data_to_db(None, price_df)
        self.assertFalse(result)

    def test_returns_false_when_settlement_load_fails(self):
        '''Test that function returns False when settlement loading fails.'''
        mock_connection = Mock()

        price_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'system_sell_price': [50.5]
        })

        with patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = None
            result = load_price_data_to_db(mock_connection, price_df)

        self.assertFalse(result)

    def test_handles_integrity_error(self):
        '''Test that integrity errors are handled.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        price_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'system_sell_price': [50.5]
        })

        with patch('load_elexon.execute_values') as mock_execute, \
             patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1]
            mock_execute.side_effect = psycopg2.IntegrityError("Constraint violation")

            result = load_price_data_to_db(mock_connection, price_df)

        self.assertFalse(result)
        mock_connection.rollback.assert_called()

    def test_deduplicates_prices(self):
        '''Test that duplicate prices are deduplicated keeping last value.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        price_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1)],
            'settlement_period': [1, 1, 1],
            'system_sell_price': [50.0, 55.0, 60.0]  # Last value should be kept
        })

        with patch('load_elexon.execute_values') as mock_execute, \
             patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = [1]
            mock_execute.return_value = None

            load_price_data_to_db(mock_connection, price_df)

            # Verify execute_values was called with deduplicated data (should have 1 record)
            calls = mock_execute.call_args_list
            # The second call is for price insert (first call is for settlement in the mocked function)
            price_data = calls[-1][0][2]
            self.assertEqual(len(price_data), 1)
            self.assertEqual(price_data[0][1], 60.0)  # Last value


class TestLoadFuelTypesToDb(unittest.TestCase):
    '''Tests for load_fuel_types_to_db function.'''

    def test_loads_fuel_types_successfully(self):
        '''Test that fuel types are loaded and IDs are returned.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        generation_df = pd.DataFrame({
            'fuel_type': ['WIND', 'SOLAR', 'WIND'],
            'generation': [100, 50, 110]
        })

        with patch('load_elexon.execute_values') as mock_execute:
            mock_execute.return_value = [
                (1, 'WIND'),
                (2, 'SOLAR')
            ]
            result = load_fuel_types_to_db(mock_connection, generation_df)

        # Should return IDs for each row in original order
        self.assertEqual(result, [1, 2, 1])

    def test_returns_none_when_no_connection(self):
        '''Test that function returns None when connection is None.'''
        generation_df = pd.DataFrame({
            'fuel_type': ['WIND'],
            'generation': [100]
        })
        result = load_fuel_types_to_db(None, generation_df)
        self.assertIsNone(result)

    def test_handles_integrity_error(self):
        '''Test that integrity errors are handled.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        generation_df = pd.DataFrame({
            'fuel_type': ['WIND'],
            'generation': [100]
        })

        with patch('load_elexon.execute_values') as mock_execute:
            mock_execute.side_effect = psycopg2.IntegrityError("Constraint violation")
            result = load_fuel_types_to_db(mock_connection, generation_df)

        self.assertIsNone(result)
        mock_connection.rollback.assert_called_once()


class TestLoadGenerationDataToDb(unittest.TestCase):
    '''Tests for load_generation_data_to_db function.'''

    def test_loads_generation_data_successfully(self):
        '''Test that generation data is loaded successfully.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        generation_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 2)],
            'settlement_period': [1, 2],
            'fuel_type': ['WIND', 'SOLAR'],
            'generation': [100.5, 50.3]
        })

        with patch('load_elexon.execute_values') as mock_execute, \
             patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement, \
             patch('load_elexon.load_fuel_types_to_db') as mock_load_fuel:
            mock_load_settlement.return_value = [1, 2]
            mock_load_fuel.return_value = [1, 2]
            mock_execute.return_value = None

            result = load_generation_data_to_db(mock_connection, generation_df)

        self.assertTrue(result)
        mock_connection.commit.assert_called_once()

    def test_returns_false_when_no_connection(self):
        '''Test that function returns False when connection is None.'''
        generation_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'fuel_type': ['WIND'],
            'generation': [100.5]
        })
        result = load_generation_data_to_db(None, generation_df)
        self.assertFalse(result)

    def test_returns_false_when_settlement_load_fails(self):
        '''Test that function returns False when settlement loading fails.'''
        mock_connection = Mock()

        generation_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'fuel_type': ['WIND'],
            'generation': [100.5]
        })

        with patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement:
            mock_load_settlement.return_value = None
            result = load_generation_data_to_db(mock_connection, generation_df)

        self.assertFalse(result)

    def test_returns_false_when_fuel_type_load_fails(self):
        '''Test that function returns False when fuel type loading fails.'''
        mock_connection = Mock()

        generation_df = pd.DataFrame({
            'date': [date(2023, 1, 1)],
            'settlement_period': [1],
            'fuel_type': ['WIND'],
            'generation': [100.5]
        })

        with patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement, \
             patch('load_elexon.load_fuel_types_to_db') as mock_load_fuel:
            mock_load_settlement.return_value = [1]
            mock_load_fuel.return_value = None
            result = load_generation_data_to_db(mock_connection, generation_df)

        self.assertFalse(result)

    def test_deduplicates_generation_data(self):
        '''Test that duplicate generation records are deduplicated keeping last value.'''
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        generation_df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1)],
            'settlement_period': [1, 1, 1],
            'fuel_type': ['WIND', 'WIND', 'WIND'],
            'generation': [100.0, 105.0, 110.0]  # Last value should be kept
        })

        with patch('load_elexon.execute_values') as mock_execute, \
             patch('load_elexon.load_settlement_data_to_db') as mock_load_settlement, \
             patch('load_elexon.load_fuel_types_to_db') as mock_load_fuel:
            mock_load_settlement.return_value = [1]
            mock_load_fuel.return_value = [1]
            mock_execute.return_value = None

            load_generation_data_to_db(mock_connection, generation_df)

            # Verify execute_values was called with deduplicated data
            calls = mock_execute.call_args_list
            generation_data = calls[-1][0][2]
            self.assertEqual(len(generation_data), 1)
            self.assertEqual(generation_data[0][2], 110.0)  # Last value


if __name__ == '__main__':
    unittest.main()
