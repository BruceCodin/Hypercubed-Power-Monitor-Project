'''Simple test suite for elexon transform module.'''
import unittest
import pandas as pd
from datetime import date
from transform import (
    update_price_column_names,
    expand_generation_data_column,
    add_date_column_to_generation
)


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
                [{'psrType': 'WIND', 'quantity': 100}, {'psrType': 'SOLAR', 'quantity': 50}],
                [{'psrType': 'WIND', 'quantity': 110}]
            ]
        })
        result = expand_generation_data_column(df)
        self.assertGreater(len(result), len(df))
        self.assertIn('psrType', result.columns)
        self.assertIn('quantity', result.columns)
        self.assertEqual(len(result), 3)

    def test_handles_string_data(self):
        '''Test that string data is parsed correctly.'''
        df = pd.DataFrame({
            'data': ["[{'psrType': 'WIND', 'quantity': 100}]"]
        })
        result = expand_generation_data_column(df)
        self.assertIn('psrType', result.columns)
        self.assertEqual(result['psrType'].iloc[0], 'WIND')

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['data'])
        result = expand_generation_data_column(df)
        self.assertTrue(result.empty)


class TestAddDateColumnToGeneration(unittest.TestCase):
    '''Tests for add_date_column_to_generation function.'''

    def test_adds_settlement_date_column(self):
        '''Test that settlement_date column is added correctly.'''
        df = pd.DataFrame({
            'startTime': ['2023-01-01T00:00:00Z', '2023-01-02T00:00:00Z'],
            'quantity': [100, 110]
        })
        result = add_date_column_to_generation(df)
        self.assertIn('settlement_date', result.columns)
        self.assertNotIn('startTime', result.columns)
        self.assertEqual(result['settlement_date'].iloc[0], date(2023, 1, 1))
        self.assertEqual(result['settlement_date'].iloc[1], date(2023, 1, 2))

    def test_preserves_other_columns(self):
        '''Test that other columns are preserved.'''
        df = pd.DataFrame({
            'startTime': ['2023-01-01T00:00:00Z'],
            'quantity': [100],
            'psrType': ['WIND']
        })
        result = add_date_column_to_generation(df)
        self.assertIn('quantity', result.columns)
        self.assertIn('psrType', result.columns)
        self.assertEqual(result['quantity'].iloc[0], 100)
        self.assertEqual(result['psrType'].iloc[0], 'WIND')

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['startTime'])
        result = add_date_column_to_generation(df)
        self.assertTrue(result.empty)


if __name__ == '__main__':
    unittest.main()
