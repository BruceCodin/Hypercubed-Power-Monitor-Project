"""Unit tests for S3 upload functions."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch
import pandas as pd
import pytest
from load_gen_to_s3 import upload_data_to_s3, POWER_CUT_S3_PATH


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'outage_id': [1, 2],
        'source_provider': ['SSEN', 'National Grid'],
        'status': ['Active', 'Restored'],
        'recording_time': ['2025-01-15 10:00:00', '2025-01-15 11:00:00'],
        'postcode': ['AB10', 'SW1A']
    })


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_calls_to_parquet(mock_to_parquet, sample_dataframe):
    """Test that awswrangler to_parquet is called."""
    upload_data_to_s3(sample_dataframe)
    mock_to_parquet.assert_called_once()


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_uses_correct_path(mock_to_parquet, sample_dataframe):
    """Test that data is uploaded to correct S3 path."""
    upload_data_to_s3(sample_dataframe)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['path'] == POWER_CUT_S3_PATH


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_creates_partitions(mock_to_parquet, sample_dataframe):
    """Test that data is partitioned by year, month, day."""
    upload_data_to_s3(sample_dataframe)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['partition_cols'] == ['year', 'month', 'day']


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_uses_dataset_mode(mock_to_parquet, sample_dataframe):
    """Test that dataset mode is enabled."""
    upload_data_to_s3(sample_dataframe)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['dataset'] is True


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_uses_overwrite_mode(mock_to_parquet, sample_dataframe):
    """Test that overwrite mode is used."""
    upload_data_to_s3(sample_dataframe)

    call_kwargs = mock_to_parquet.call_args[1]
    assert call_kwargs['mode'] == 'overwrite'


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_adds_year_column(mock_to_parquet, sample_dataframe):
    """Test that year column is extracted from recording_time."""
    upload_data_to_s3(sample_dataframe)

    uploaded_df = mock_to_parquet.call_args[1]['df']
    assert 'year' in uploaded_df.columns
    assert uploaded_df['year'].iloc[0] == 2025


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_adds_month_column(mock_to_parquet, sample_dataframe):
    """Test that month column is extracted from recording_time."""
    upload_data_to_s3(sample_dataframe)

    uploaded_df = mock_to_parquet.call_args[1]['df']
    assert 'month' in uploaded_df.columns
    assert uploaded_df['month'].iloc[0] == 1


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_adds_day_column(mock_to_parquet, sample_dataframe):
    """Test that day column is extracted from recording_time."""
    upload_data_to_s3(sample_dataframe)

    uploaded_df = mock_to_parquet.call_args[1]['df']
    assert 'day' in uploaded_df.columns
    assert uploaded_df['day'].iloc[0] == 15


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_does_not_modify_original(
        mock_to_parquet, sample_dataframe):
    """Test that original DataFrame is not modified."""
    original_columns = sample_dataframe.columns.tolist()
    upload_data_to_s3(sample_dataframe)

    assert sample_dataframe.columns.tolist() == original_columns
    assert 'year' not in sample_dataframe.columns


@patch('load_gen_to_s3.wr.s3.to_parquet')
def test_upload_data_to_s3_preserves_original_data(
        mock_to_parquet, sample_dataframe):
    """Test that original data values are preserved in upload."""
    upload_data_to_s3(sample_dataframe)

    uploaded_df = mock_to_parquet.call_args[1]['df']
    assert uploaded_df['outage_id'].tolist() == [1, 2]
    assert uploaded_df['source_provider'].tolist() == ['SSEN', 'National Grid']
