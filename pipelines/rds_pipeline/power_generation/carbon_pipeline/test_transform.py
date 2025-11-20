'''Simple test suite for transform module.'''
import unittest
import pandas as pd
from datetime import datetime
from unittest.mock import patch
from transform import (
    add_settlement_period,
    refactor_intensity_column,
    add_date_column,
    transform_carbon_data,
    update_column_names
)


class TestAddSettlementPeriod(unittest.TestCase):
    '''Tests for add_settlement_period function.'''

    def test_adds_settlement_period_successfully(self):
        '''Test that settlement periods are added correctly.'''
        df = pd.DataFrame({
            'from': ['2025-01-01T00:00Z', '2025-01-01T00:30Z', '2025-01-01T01:00Z'],
            'to': ['2025-01-01T00:30Z', '2025-01-01T01:00Z', '2025-01-01T01:30Z']
        })
        result = add_settlement_period(df)
        self.assertIn('settlement_period', result.columns)
        self.assertEqual(list(result['settlement_period']), [1, 2, 3])

    def test_raises_error_for_missing_columns(self):
        '''Test that ValueError is raised when columns are missing.'''
        df = pd.DataFrame({'other': [1, 2, 3]})
        with self.assertRaises(ValueError):
            add_settlement_period(df)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['from', 'to'])
        result = add_settlement_period(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            add_settlement_period("not a dataframe")


class TestRefactorIntensityColumn(unittest.TestCase):
    '''Tests for refactor_intensity_column function.'''

    def test_extracts_intensity_data(self):
        '''Test that intensity column is refactored correctly.'''
        df = pd.DataFrame({
            'intensity': [
                {'forecast': 100, 'actual': 95},
                {'forecast': 110, 'actual': 105}
            ]
        })
        result = refactor_intensity_column(df)
        self.assertNotIn('intensity', result.columns)
        self.assertIn('forecast', result.columns)
        self.assertIn('actual', result.columns)
        self.assertEqual(list(result['forecast']), [100, 110])
        self.assertEqual(list(result['actual']), [95, 105])

    def test_raises_error_for_missing_intensity_column(self):
        '''Test that ValueError is raised when intensity column is missing.'''
        df = pd.DataFrame({'other': [1, 2, 3]})
        with self.assertRaises(ValueError):
            refactor_intensity_column(df)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['intensity'])
        result = refactor_intensity_column(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            refactor_intensity_column("not a dataframe")


class TestAddDateColumn(unittest.TestCase):
    '''Tests for add_date_column function.'''

    def test_adds_date_column_and_drops_timestamp_columns(self):
        '''Test that date column is added and timestamp columns are dropped.'''
        df = pd.DataFrame({
            'from': ['2025-01-01T00:00Z', '2025-01-02T00:00Z'],
            'to': ['2025-01-01T00:30Z', '2025-01-02T00:30Z']
        })
        result = add_date_column(df)
        self.assertIn('date', result.columns)
        self.assertNotIn('from', result.columns)
        self.assertNotIn('to', result.columns)
        self.assertEqual(str(result['date'].iloc[0]), '2025-01-01')
        self.assertEqual(str(result['date'].iloc[1]), '2025-01-02')

    def test_raises_error_for_missing_from_column(self):
        '''Test that ValueError is raised when from column is missing.'''
        df = pd.DataFrame({'other': [1, 2, 3]})
        with self.assertRaises(ValueError):
            add_date_column(df)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['from'])
        result = add_date_column(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            add_date_column("not a dataframe")


class TestTransformCarbonData(unittest.TestCase):
    '''Tests for transform_carbon_data function.'''

    @patch('transform.fetch_carbon_intensity_data')
    def test_transforms_data_successfully(self, mock_fetch):
        '''Test that data is transformed through the entire pipeline.'''
        mock_fetch.return_value = pd.DataFrame({
            'from': ['2025-01-01T00:00Z', '2025-01-01T00:30Z'],
            'to': ['2025-01-01T00:30Z', '2025-01-01T01:00Z'],
            'intensity': [
                {'forecast': 100, 'actual': 95, 'index': 'moderate'},
                {'forecast': 110, 'actual': 105, 'index': 'high'}
            ]
        })

        from_dt = datetime(2025, 1, 1, 0, 0)
        to_dt = datetime(2025, 1, 1, 1, 0)
        result = transform_carbon_data(from_dt, to_dt)

        self.assertIn('date', result.columns)
        self.assertIn('settlement_period', result.columns)
        self.assertIn('intensity_forecast', result.columns)
        self.assertIn('intensity_actual', result.columns)
        self.assertIn('carbon_index', result.columns)
        self.assertEqual(len(result), 2)

    def test_raises_error_for_invalid_datetime_type(self):
        '''Test that TypeError is raised for non-datetime input.'''
        with self.assertRaises(TypeError):
            transform_carbon_data("not a datetime", datetime.now())

    def test_raises_error_when_from_after_to(self):
        '''Test that ValueError is raised when from_datetime is after to_datetime.'''
        from_dt = datetime(2025, 1, 2, 0, 0)
        to_dt = datetime(2025, 1, 1, 0, 0)
        with self.assertRaises(ValueError):
            transform_carbon_data(from_dt, to_dt)

    @patch('transform.fetch_carbon_intensity_data')
    def test_handles_empty_data_from_api(self, mock_fetch):
        '''Test that empty DataFrame from API is handled gracefully.'''
        mock_fetch.return_value = pd.DataFrame()

        from_dt = datetime(2025, 1, 1, 0, 0)
        to_dt = datetime(2025, 1, 1, 1, 0)
        result = transform_carbon_data(from_dt, to_dt)

        self.assertTrue(result.empty)


class TestUpdateColumnNames(unittest.TestCase):
    '''Tests for update_column_names function.'''

    def test_updates_column_names_successfully(self):
        '''Test that column names are updated to match database schema.'''
        df = pd.DataFrame({
            'forecast': [100, 110],
            'actual': [95, 105],
            'index': ['moderate', 'high']
        })
        result = update_column_names(df)
        self.assertIn('intensity_forecast', result.columns)
        self.assertIn('intensity_actual', result.columns)
        self.assertIn('carbon_index', result.columns)
        self.assertNotIn('forecast', result.columns)
        self.assertNotIn('actual', result.columns)
        self.assertNotIn('index', result.columns)
        self.assertEqual(list(result['intensity_forecast']), [100, 110])
        self.assertEqual(list(result['intensity_actual']), [95, 105])

    def test_handles_missing_columns_gracefully(self):
        '''Test that function works even if some columns are missing.'''
        df = pd.DataFrame({
            'forecast': [100, 110],
            'other': [1, 2]
        })
        result = update_column_names(df)
        self.assertIn('intensity_forecast', result.columns)
        self.assertIn('other', result.columns)
        self.assertNotIn('forecast', result.columns)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame()
        result = update_column_names(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            update_column_names("not a dataframe")


if __name__ == '__main__':
    unittest.main()
