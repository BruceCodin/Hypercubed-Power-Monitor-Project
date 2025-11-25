'''Simple test suite for elexon transform module.'''
import unittest
import pandas as pd
from datetime import date
from transform_elexon import (
    update_price_column_names,
    expand_generation_data_column,
    add_date_column_to_generation
)
# pylint: skip-file
# pragma: no cover


class TestUpdatePriceColumnNames(unittest.TestCase):
    '''Tests for update_price_column_names function.'''

    def test_updates_column_names_successfully(self):
        '''Test that price column names are updated correctly.'''
        df = pd.DataFrame({
            'settlementDate': ['2023-01-01', '2023-01-02'],
            'settlementPeriod': [1, 2],
            'systemSellPrice': [50.5, 60.3]
        })
        result = update_price_column_names(df)
        self.assertIn('date', result.columns)
        self.assertIn('settlement_period', result.columns)
        self.assertIn('system_sell_price', result.columns)
        self.assertNotIn('settlementDate', result.columns)
        self.assertNotIn('settlementPeriod', result.columns)
        self.assertNotIn('systemSellPrice', result.columns)

        # Check that date column is datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date']))

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame()
        result = update_price_column_names(df)
        self.assertTrue(result.empty)

    def test_preserves_other_columns(self):
        '''Test that other columns are preserved.'''
        df = pd.DataFrame({
            'settlementDate': ['2023-01-01'],
            'settlementPeriod': [1],
            'systemSellPrice': [50.5],
            'other_column': ['value']
        })
        result = update_price_column_names(df)
        self.assertIn('other_column', result.columns)
        self.assertEqual(result['other_column'].iloc[0], 'value')


class TestExpandGenerationDataColumn(unittest.TestCase):
    '''Tests for expand_generation_data_column function.'''

    def test_expands_data_column_successfully(self):
        '''Test that data column is expanded correctly.'''
        df = pd.DataFrame({
            'data': [
                [{'fuelType': 'WIND', 'quantity': 100}, {
                    'fuelType': 'SOLAR', 'quantity': 50}],
                [{'fuelType': 'WIND', 'quantity': 110}]
            ]
        })
        result = expand_generation_data_column(df)
        self.assertGreater(len(result), len(df))
        self.assertIn('fuel_type', result.columns)
        self.assertNotIn('fuelType', result.columns)
        self.assertIn('quantity', result.columns)
        self.assertEqual(len(result), 3)

    def test_handles_string_data(self):
        '''Test that string data is parsed correctly.'''
        df = pd.DataFrame({
            'data': ["[{'fuelType': 'WIND', 'quantity': 100}]"]
        })
        result = expand_generation_data_column(df)
        self.assertIn('fuel_type', result.columns)
        self.assertEqual(result['fuel_type'].iloc[0], 'WIND')

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['data'])
        result = expand_generation_data_column(df)
        self.assertTrue(result.empty)


class TestAddDateColumnToGeneration(unittest.TestCase):
    '''Tests for add_date_column_to_generation function.'''

    def test_adds_settlement_date_column(self):
        '''Test that date column is added correctly.'''
        df = pd.DataFrame({
            'startTime': ['2023-01-01T00:00:00Z', '2023-01-02T00:00:00Z'],
            'quantity': [100, 110],
            'settlementPeriod': [1, 2]
        })
        result = add_date_column_to_generation(df)
        self.assertIn('date', result.columns)  # Check
        self.assertIn('settlement_period', result.columns)
        self.assertNotIn('startTime', result.columns)
        self.assertNotIn('settlementPeriod', result.columns)

        # Check that date is datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(
            result['date']))

        # Check values are correct
        self.assertEqual(result['date'].iloc[0],
                         pd.Timestamp('2023-01-01'))
        self.assertEqual(result['date'].iloc[1],
                         pd.Timestamp('2023-01-02'))

    def test_preserves_other_columns(self):
        '''Test that other columns are preserved.'''
        df = pd.DataFrame({
            'startTime': ['2023-01-01T00:00:00Z'],
            'quantity': [100],
            'psrType': ['WIND'],
            'settlementPeriod': [1]
        })
        result = add_date_column_to_generation(df)
        self.assertIn('quantity', result.columns)
        self.assertIn('psrType', result.columns)
        self.assertIn('settlement_period', result.columns)
        self.assertEqual(result['quantity'].iloc[0], 100)
        self.assertEqual(result['psrType'].iloc[0], 'WIND')
        self.assertEqual(result['settlement_period'].iloc[0], 1)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['startTime'])
        result = add_date_column_to_generation(df)
        self.assertTrue(result.empty)
