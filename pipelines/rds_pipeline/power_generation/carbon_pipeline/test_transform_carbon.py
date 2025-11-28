'''Simple test suite for transform module.'''
import unittest
import pandas as pd
from datetime import date
from transform_carbon import (
    add_settlement_period,
    refactor_intensity_column,
    add_date_column,
    transform_carbon_data,
    update_column_names,
    make_date_datetime,
    remove_null_intensities
)
# pylint: skip-file
# pragma: no cover


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

    def test_transforms_data_successfully(self):
        '''Test that data is transformed through the entire pipeline.'''
        df = pd.DataFrame({
            'from': ['2025-01-01T00:00Z', '2025-01-01T00:30Z'],
            'to': ['2025-01-01T00:30Z', '2025-01-01T01:00Z'],
            'intensity': [
                {'forecast': 100, 'actual': 95, 'index': 'moderate'},
                {'forecast': 110, 'actual': 105, 'index': 'high'}
            ]
        })

        result = transform_carbon_data(df)

        self.assertIn('date', result.columns)
        self.assertIn('settlement_period', result.columns)
        self.assertIn('intensity_forecast', result.columns)
        self.assertIn('intensity_actual', result.columns)
        self.assertIn('carbon_index', result.columns)
        self.assertEqual(len(result), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date']))

    def test_raises_error_for_invalid_dataframe_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            transform_carbon_data("not a dataframe")

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame()
        result = transform_carbon_data(df)
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


class TestMakeDateDatetime(unittest.TestCase):
    '''Tests for make_date_datetime function.'''

    def test_converts_date_to_datetime(self):
        '''Test that date column is converted to datetime.'''
        df = pd.DataFrame({
            'date': [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            'intensity_forecast': [100, 110, 120]
        })
        result = make_date_datetime(df)

        # Check that date is datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date']))

        # Check values are correct
        self.assertEqual(result['date'].iloc[0], pd.Timestamp('2023-01-01'))
        self.assertEqual(result['date'].iloc[1], pd.Timestamp('2023-01-02'))

    def test_raises_error_for_missing_date_column(self):
        '''Test that ValueError is raised when date column is missing.'''
        df = pd.DataFrame({
            'other_column': [1, 2, 3]
        })
        with self.assertRaises(ValueError):
            make_date_datetime(df)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame()
        result = make_date_datetime(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            make_date_datetime("not a dataframe")

    def test_handles_already_datetime_column(self):
        '''Test that function works even if column is already datetime.'''
        df = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'intensity_forecast': [100, 110]
        })
        result = make_date_datetime(df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date']))


class TestRemoveNullIntensities(unittest.TestCase):
    '''Tests for remove_null_intensities function.'''

    def test_removes_rows_with_null_actual_column(self):
        '''Test that rows with null actual values are removed.'''
        df = pd.DataFrame({
            'actual': [95, None, 105, None],
            'forecast': [100, 110, 120, 130]
        })
        result = remove_null_intensities(df)
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result['actual']), [95, 105])

    def test_removes_rows_with_null_intensity_actual_column(self):
        '''Test that rows with null intensity_actual values are removed.'''
        df = pd.DataFrame({
            'intensity_actual': [95, None, 105],
            'intensity_forecast': [100, 110, 120]
        })
        result = remove_null_intensities(df)
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result['intensity_actual']), [95, 105])

    def test_handles_all_null_values(self):
        '''Test that all rows are removed if all have null values.'''
        df = pd.DataFrame({
            'actual': [None, None, None],
            'forecast': [100, 110, 120]
        })
        result = remove_null_intensities(df)
        self.assertEqual(len(result), 0)
        self.assertTrue(result.empty)

    def test_handles_no_null_values(self):
        '''Test that no rows are removed if none have null values.'''
        df = pd.DataFrame({
            'actual': [95, 105, 115],
            'forecast': [100, 110, 120]
        })
        result = remove_null_intensities(df)
        self.assertEqual(len(result), 3)

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame(columns=['actual', 'forecast'])
        result = remove_null_intensities(df)
        self.assertTrue(result.empty)

    def test_handles_missing_actual_column(self):
        '''Test that function handles missing actual/intensity_actual column.'''
        df = pd.DataFrame({
            'forecast': [100, 110, 120]
        })
        result = remove_null_intensities(df)
        self.assertEqual(len(result), 3)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            remove_null_intensities("not a dataframe")


if __name__ == '__main__':
    unittest.main()
