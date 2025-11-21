"""Unit tests for SSEN power cut extraction functions."""
# pylint: skip-file
# pragma: no cover

from unittest.mock import patch, Mock
import requests as req
from extract_ssen import (
    extract_power_cut_data,
    parse_power_cut_data,
    extract_ssen_data,
    PROVIDER
)


def test_parse_valid_data_with_multiple_faults():
    """Test parsing valid data with multiple faults."""
    mock_data = {
        "Faults": [
            {
                "type": "PSI",
                "loggedAt": "2025-01-15T10:30:00",
                "affectedAreas": ["AB10", "AB11"]
            },
            {
                "type": "Unplanned",
                "loggedAt": "2025-01-15T11:00:00",
                "affectedAreas": ["IV1"]
            }
        ]
    }
    result = parse_power_cut_data(mock_data)
    assert len(result) == 2
    assert result[0]["status"] == "PSI"
    assert result[1]["affected_postcodes"] == ["IV1"]


def test_parse_none_or_empty_data():
    """Test parsing None or empty data returns empty list."""
    assert parse_power_cut_data(None) == []
    assert parse_power_cut_data({}) == []
    assert parse_power_cut_data({"Faults": []}) == []


def test_parse_data_without_faults_key():
    """Test parsing data without 'Faults' key returns empty list."""
    result = parse_power_cut_data({"OtherKey": "value"})
    assert result == []


def test_parse_fault_with_missing_fields():
    """Test parsing fault entries with missing fields."""
    mock_data = {
        "Faults": [
            {"type": "Active"},
            {"loggedAt": "2025-01-15T13:00:00"}
        ]
    }
    result = parse_power_cut_data(mock_data)
    assert result[0]["status"] == "Active"
    assert result[0]["outage_date"] is None
    assert result[1]["status"] is None


def test_parse_data_includes_correct_provider():
    """Test all entries have correct source provider."""
    mock_data = {"Faults": [{"type": "Active"}]}
    result = parse_power_cut_data(mock_data)
    assert result[0]["source_provider"] == PROVIDER


@patch('extract_ssen.req.get')
def test_extract_power_cut_data_success(mock_get):
    """Test successful API data extraction."""
    mock_response = Mock()
    mock_response.json.return_value = {"Faults": []}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    result = extract_power_cut_data()
    assert result == {"Faults": []}
    mock_get.assert_called_once()


@patch('extract_ssen.req.get')
def test_extract_power_cut_data_api_failure(mock_get):
    """Test API failure returns None."""
    mock_get.side_effect = req.exceptions.RequestException("API Error")
    result = extract_power_cut_data()
    assert result is None


@patch('extract_ssen.extract_power_cut_data')
def test_extract_ssen_data_success(mock_extract):
    """Test main extraction function success path."""
    mock_extract.return_value = {"Faults": [{"type": "PSI"}]}
    result = extract_ssen_data()
    assert isinstance(result, list)


@patch('extract_ssen.extract_power_cut_data')
def test_extract_ssen_data_api_failure(mock_extract):
    """Test main extraction function when API fails."""
    mock_extract.return_value = None
    result = extract_ssen_data()
    assert result is None
