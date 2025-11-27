'''Tests for the Elexon data extraction module.'''
from datetime import datetime
import pandas as pd
import pytest
from extract_elexon import fetch_elexon_price_data, parse_elexon_price_data, fetch_elexon_generation_data
# pylint: skip-file
# pragma: no cover


class TestParseElexonPriceData:
    '''Tests for parse_elexon_price_data function'''

    def test_parse_price_data_returns_dataframe(self):
        '''Test that parse_elexon_price_data returns a DataFrame'''
        mock_data = {
            'data': [
                {'settlementDate': '2024-01-01',
                    'settlementPeriod': 1, 'systemSellPrice': 90.0},
                {'settlementDate': '2024-01-01',
                    'settlementPeriod': 2, 'systemSellPrice': 100.0}
            ]
        }
        result = parse_elexon_price_data(mock_data)
        assert isinstance(result, pd.DataFrame)

    def test_parse_price_data_has_correct_columns(self):
        '''Test that the returned DataFrame has the correct columns'''
        mock_data = {
            'data': [
                {'settlementDate': '2024-01-01', 'settlementPeriod': 1,
                    'systemSellPrice': 90.0, 'extraColumn': 'ignore'},
                {'settlementDate': '2024-01-01', 'settlementPeriod': 2,
                    'systemSellPrice': 100.0, 'extraColumn': 'ignore'}
            ]
        }
        result = parse_elexon_price_data(mock_data)
        expected_columns = ['settlementDate',
                            'settlementPeriod', 'systemSellPrice']
        assert list(result.columns) == expected_columns

    def test_parse_price_data_correct_row_count(self):
        '''Test that the returned DataFrame has the correct number of rows'''
        mock_data = {
            'data': [
                {'settlementDate': '2024-01-01',
                    'settlementPeriod': 1, 'systemSellPrice': 90.0},
                {'settlementDate': '2024-01-01',
                    'settlementPeriod': 2, 'systemSellPrice': 100.0},
                {'settlementDate': '2024-01-01',
                    'settlementPeriod': 3, 'systemSellPrice': 110.0}
            ]
        }
        result = parse_elexon_price_data(mock_data)
        assert len(result) == 3


class TestFetchElexonPriceData:
    '''Tests for fetch_elexon_price_data function'''

    def test_fetch_price_data_with_invalid_date_type(self):
        '''Test that fetch_elexon_price_data raises ValueError for non-datetime input'''
        with pytest.raises(ValueError, match="date must be a datetime object"):
            fetch_elexon_price_data("2024-01-01")


class TestFetchElexonGenerationData:
    '''Tests for fetch_elexon_generation_data function'''

    def test_fetch_generation_data_with_invalid_date_types(self):
        '''Test that fetch_elexon_generation_data raises ValueError for non-datetime inputs'''
        with pytest.raises(ValueError, match="startTime and endTime must be datetime objects"):
            fetch_elexon_generation_data("2024-01-01", datetime(2024, 1, 2))

        with pytest.raises(ValueError, match="startTime and endTime must be datetime objects"):
            fetch_elexon_generation_data(datetime(2024, 1, 1), "2024-01-02")
