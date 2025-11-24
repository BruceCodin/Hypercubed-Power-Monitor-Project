"""Unit tests for S3 upload functions."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch
import pandas as pd
import pytest
from load_to_s3 import (
    prepare_carbon_data,
    prepare_energy_generation_data,
    upload_carbon_data_to_s3,
    upload_energy_generation_data_to_s3,
    CARBON_S3_PATH,
    ENERGY_GENERATION_S3_PATH
)


@pytest.fixture
def sample_carbon_dataframe():
    """Create a sample carbon intensity DataFrame for testing."""
    return pd.DataFrame({
        'intensity_id': [1, 2],
        'intensity_forecast': [150, 200],
        'intensity_actual': [145, 195],
        'intensity_index': ['moderate', 'high'],
        'settlement_id': [100, 101],
        'settlement_date': ['2025-01-15', '2025-01-16'],
        'settlement_period': [1, 2]
    })


@pytest.fixture
def sample_generation_dataframe():
    """Create a sample power generation DataFrame for testing."""
    return pd.DataFrame({
        'generation_id': [1, 2],
        'fuel_type_id': [2, 3],
        'generation_mw': [1500.5, 2000.0],
        'settlement_id': [100, 101],
        'settlement_period': [1, 2],
        'settlement_date': ['2025-01-15', '2025-01-16']
    })


def test_prepare_carbon_data_adds_year_column(sample_carbon_dataframe):
    """Test that year column is extracted from settlement_date."""
    result = prepare_carbon_data(sample_carbon_dataframe)

    assert 'year' in result.columns
    assert result['year'].iloc[0] == 2025


def test_prepare_carbon_data_adds_month_column(sample_carbon_dataframe):
    """Test that month column is extracted from settlement_date."""
    result = prepare_carbon_data(sample_carbon_dataframe)

    assert 'month' in result.columns
    assert result['month'].iloc[0] == 1


def test_prepare_carbon_data_adds_day_column(sample_carbon_dataframe):
    """Test that day column is extracted from settlement_date."""
    result = prepare_carbon_data(sample_carbon_dataframe)

    assert 'day' in result.columns
    assert result['day'].iloc[0] == 15


def test_prepare_carbon_data_does_not_modify_original(sample_carbon_dataframe):
    """Test that original DataFrame is not modified."""
    original_columns = sample_carbon_dataframe.columns.tolist()
    prepare_carbon_data(sample_carbon_dataframe)

    assert sample_carbon_dataframe.columns.tolist() == original_columns
    assert 'year' not in sample_carbon_dataframe.columns


def test_prepare_carbon_data_preserves_original_data(sample_carbon_dataframe):
    """Test that original data values are preserved."""
    result = prepare_carbon_data(sample_carbon_dataframe)

    assert result['intensity_id'].tolist() == [1, 2]
    assert result['intensity_forecast'].tolist() == [150, 200]
    assert result['intensity_actual'].tolist() == [145, 195]


def test_prepare_energy_generation_data_adds_year_column(
        sample_generation_dataframe):
    """Test that year column is extracted from settlement_date."""
    result = prepare_energy_generation_data(sample_generation_dataframe)

    assert 'year' in result.columns
    assert result['year'].iloc[0] == 2025


def test_prepare_energy_generation_data_adds_month_column(
        sample_generation_dataframe):
    """Test that month column is extracted from settlement_date."""
    result = prepare_energy_generation_data(sample_generation_dataframe)

    assert 'month' in result.columns
    assert result['month'].iloc[0] == 1


def test_prepare_energy_generation_data_adds_day_column(
        sample_generation_dataframe):
    """Test that day column is extracted from settlement_date."""
    result = prepare_energy_generation_data(sample_generation_dataframe)

    assert 'day' in result.columns
    assert result['day'].iloc[0] == 15


def test_prepare_energy_generation_data_does_not_modify_original(
        sample_generation_dataframe):
    """Test that original DataFrame is not modified."""
    original_columns = sample_generation_dataframe.columns.tolist()
    prepare_energy_generation_data(sample_generation_dataframe)

    assert sample_generation_dataframe.columns.tolist() == original_columns
    assert 'year' not in sample_generation_dataframe.columns


def test_prepare_energy_generation_data_preserves_original_data(
        sample_generation_dataframe):
    """Test that original data values are preserved."""
    result = prepare_energy_generation_data(sample_generation_dataframe)

    assert result['generation_id'].tolist() == [1, 2]
    assert result['fuel_type_id'].tolist() == [2, 3]
    assert result['generation_mw'].tolist() == [1500.5, 2000.0]


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_carbon_data_to_s3_calls_to_parquet(
        mock_to_parquet, sample_carbon_dataframe):
    """Test that awswrangler to_parquet is called."""
    prepared_data = prepare_carbon_data(sample_carbon_dataframe)
    upload_carbon_data_to_s3(prepared_data)
    mock_to_parquet.assert_called_once()


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_carbon_data_to_s3_uses_correct_path(
        mock_to_parquet, sample_carbon_dataframe):
    """Test that data is uploaded to correct S3 path."""
    prepared_data = prepare_carbon_data(sample_carbon_dataframe)
    upload_carbon_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['path'] == CARBON_S3_PATH


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_carbon_data_to_s3_creates_partitions(
        mock_to_parquet, sample_carbon_dataframe):
    """Test that data is partitioned by year, month, day."""
    prepared_data = prepare_carbon_data(sample_carbon_dataframe)
    upload_carbon_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['partition_cols'] == ['year', 'month', 'day']


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_carbon_data_to_s3_uses_dataset_mode(
        mock_to_parquet, sample_carbon_dataframe):
    """Test that dataset mode is enabled."""
    prepared_data = prepare_carbon_data(sample_carbon_dataframe)
    upload_carbon_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['dataset'] is True


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_carbon_data_to_s3_uses_overwrite_mode(
        mock_to_parquet, sample_carbon_dataframe):
    """Test that overwrite mode is used."""
    prepared_data = prepare_carbon_data(sample_carbon_dataframe)
    upload_carbon_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['mode'] == 'overwrite'


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_energy_generation_data_to_s3_calls_to_parquet(
        mock_to_parquet, sample_generation_dataframe):
    """Test that awswrangler to_parquet is called."""
    prepared_data = prepare_energy_generation_data(sample_generation_dataframe)
    upload_energy_generation_data_to_s3(prepared_data)
    mock_to_parquet.assert_called_once()


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_energy_generation_data_to_s3_uses_correct_path(
        mock_to_parquet, sample_generation_dataframe):
    """Test that data is uploaded to correct S3 path."""
    prepared_data = prepare_energy_generation_data(sample_generation_dataframe)
    upload_energy_generation_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['path'] == ENERGY_GENERATION_S3_PATH


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_energy_generation_data_to_s3_creates_partitions(
        mock_to_parquet, sample_generation_dataframe):
    """Test that data is partitioned by year, month, day."""
    prepared_data = prepare_energy_generation_data(sample_generation_dataframe)
    upload_energy_generation_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['partition_cols'] == ['year', 'month', 'day']


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_energy_generation_data_to_s3_uses_dataset_mode(
        mock_to_parquet, sample_generation_dataframe):
    """Test that dataset mode is enabled."""
    prepared_data = prepare_energy_generation_data(sample_generation_dataframe)
    upload_energy_generation_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['dataset'] is True


@patch('load_to_s3.wr.s3.to_parquet')
def test_upload_energy_generation_data_to_s3_uses_overwrite_mode(
        mock_to_parquet, sample_generation_dataframe):
    """Test that overwrite mode is used."""
    prepared_data = prepare_energy_generation_data(sample_generation_dataframe)
    upload_energy_generation_data_to_s3(prepared_data)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['mode'] == 'overwrite'
