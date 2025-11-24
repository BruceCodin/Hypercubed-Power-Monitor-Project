'''Simple test suite for NESO transform module.'''
import unittest
import pandas as pd
from transform_neso import transform_neso_data_columns, make_date_column_datetime, validate_data_types
# pylint: skip-file
# pragma: no cover


class TestTransformNesoDataColumns(unittest.TestCase):
    '''Tests for transform_neso_data_columns function.'''

    def test_transforms_column_names_successfully(self):
        '''Test that column names are transformed correctly.'''
        df = pd.DataFrame({
            'ND': [1000, 1100, 1200],
            'TSD': [950, 1050, 1150],
            'SETTLEMENT_DATE': ['2023-01-01', '2023-01-01', '2023-01-01'],
            'SETTLEMENT_PERIOD': [1, 2, 3]
        })
        result = transform_neso_data_columns(df)

        # Check new column names exist
        self.assertIn('national_demand', result.columns)
        self.assertIn('transmission_system_demand', result.columns)
        self.assertIn('settlement_date', result.columns)
        self.assertIn('settlement_period', result.columns)

        # Check old column names are gone
        self.assertNotIn('ND', result.columns)
        self.assertNotIn('TSD', result.columns)
        self.assertNotIn('SETTLEMENT_DATE', result.columns)
        self.assertNotIn('SETTLEMENT_PERIOD', result.columns)

        # Check values are preserved
        self.assertEqual(list(result['national_demand']), [1000, 1100, 1200])
        self.assertEqual(list(result['transmission_system_demand']), [
                         950, 1050, 1150])
        self.assertEqual(list(result['settlement_period']), [1, 2, 3])

    def test_handles_missing_columns_gracefully(self):
        '''Test that function works even if some columns are missing.'''
        df = pd.DataFrame({
            'ND': [1000, 1100],
            'TSD': [950, 1050],
            'other_column': ['A', 'B']
        })
        result = transform_neso_data_columns(df)

        # Check renamed columns
        self.assertIn('national_demand', result.columns)
        self.assertIn('transmission_system_demand', result.columns)

        # Check other columns are preserved
        self.assertIn('other_column', result.columns)
        self.assertEqual(list(result['other_column']), ['A', 'B'])

    def test_handles_empty_dataframe(self):
        '''Test that empty DataFrame is handled gracefully.'''
        df = pd.DataFrame()
        result = transform_neso_data_columns(df)
        self.assertTrue(result.empty)

    def test_raises_error_for_invalid_type(self):
        '''Test that TypeError is raised for non-DataFrame input.'''
        with self.assertRaises(TypeError):
            transform_neso_data_columns("not a dataframe")

    def test_preserves_data_types(self):
        '''Test that data types are preserved after transformation.'''
        df = pd.DataFrame({
            'ND': [1000, 1100, 1200],
            'TSD': [950, 1050, 1150],
            'SETTLEMENT_DATE': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'SETTLEMENT_PERIOD': [1, 2, 3]
        })
        result = transform_neso_data_columns(df)

        self.assertEqual(result['national_demand'].dtype, df['ND'].dtype)
        self.assertEqual(
            result['transmission_system_demand'].dtype, df['TSD'].dtype)


class TestMakeDateColumnDatetime(unittest.TestCase):
    '''Tests for make_date_column_datetime function.'''

    def test_converts_settlement_date_to_datetime(self):
        '''Test that settlement_date is converted to datetime.'''
        df = pd.DataFrame({
            'settlement_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'national_demand': [1000, 1100, 1200]
        })
        result = make_date_column_datetime(df)

        # Check that settlement_date is datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(
            result['settlement_date']))

        # Check values are correct
        self.assertEqual(result['settlement_date'].iloc[0],
                         pd.Timestamp('2023-01-01'))
        self.assertEqual(result['settlement_date'].iloc[1],
                         pd.Timestamp('2023-01-02'))

    def test_raises_error_for_missing_column(self):
        '''Test that KeyError is raised when settlement_date column is missing.'''
        df = pd.DataFrame({
            'other_column': [1, 2, 3]
        })
        with self.assertRaises(KeyError):
            make_date_column_datetime(df)

    def test_handles_already_datetime_column(self):
        '''Test that function works even if column is already datetime.'''
        df = pd.DataFrame({
            'settlement_date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'national_demand': [1000, 1100]
        })
        result = make_date_column_datetime(df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(
            result['settlement_date']))


class TestValidateDataTypes(unittest.TestCase):
    '''Tests for validate_data_types function.'''

    def test_validates_correct_data_types(self):
        '''Test that valid data types return True.'''
        df = pd.DataFrame({
            'national_demand': [1000, 1100, 1200],
            'transmission_system_demand': [950, 1050, 1150],
            'settlement_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'settlement_period': [1, 2, 3]
        })
        result = validate_data_types(df)
        self.assertTrue(result)

    def test_returns_false_for_missing_column(self):
        '''Test that missing column returns False.'''
        df = pd.DataFrame({
            'national_demand': [1000, 1100],
            'transmission_system_demand': [950, 1050],
            'settlement_period': [1, 2]
        })
        result = validate_data_types(df)
        self.assertFalse(result)

    def test_returns_false_for_wrong_data_type(self):
        '''Test that wrong data type returns False.'''
        df = pd.DataFrame({
            # float instead of int
            'national_demand': [1000.5, 1100.5, 1200.5],
            'transmission_system_demand': [950, 1050, 1150],
            'settlement_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'settlement_period': [1, 2, 3]
        })
        result = validate_data_types(df)
        self.assertFalse(result)

    def test_returns_false_for_string_date_column(self):
        '''Test that string date column returns False.'''
        df = pd.DataFrame({
            'national_demand': [1000, 1100, 1200],
            'transmission_system_demand': [950, 1050, 1150],
            # string instead of datetime
            'settlement_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'settlement_period': [1, 2, 3]
        })
        result = validate_data_types(df)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
