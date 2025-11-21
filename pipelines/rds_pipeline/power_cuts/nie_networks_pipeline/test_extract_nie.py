# pylint: skip-file

"""Unit tests for NIE Networks power cut extraction functions."""

from unittest.mock import patch, Mock
import requests
from extract_NIE import (
    extract_power_cut_data,
    parse_power_cut_data,
    PROVIDER
)

# Tests for extract_power_cut_data


@patch('pipelines.rds_pipeline.power_cuts.nie_networks_pipeline.extract.req.get')
def test_extract_successful_api_call(mock_get):
    """Test successful API data extraction."""
    mock_response = Mock()
    mock_response.json.return_value = {"outageMessage": []}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    result = extract_power_cut_data()

    assert result == {"outageMessage": []}
    mock_get.assert_called_once()


@patch('pipelines.rds_pipeline.power_cuts.nie_networks_pipeline.extract.req.get')
def test_extract_failed_api_call(mock_get):
    """Test failed API call returns None."""
    mock_get.side_effect = requests.exceptions.RequestException("API Error")

    result = extract_power_cut_data()

    assert result is None


@patch('pipelines.rds_pipeline.power_cuts.nie_networks_pipeline.extract.req.get')
def test_extract_timeout_handling(mock_get):
    """Test timeout handling returns None."""
    mock_get.side_effect = requests.exceptions.Timeout("Timeout")

    result = extract_power_cut_data()

    assert result is None


# Tests for parse_power_cut_data
def test_parse_valid_data_with_multiple_outages():
    """Test parsing valid data with multiple outages."""
    mock_data = {
        "outageMessage": [
            {
                "outageType": "Unplanned",
                "startTime": "2025-01-15T10:00:00",
                "fullPostCodes": ["BT1 1AA", "BT2 2BB"]
            },
            {
                "outageType": "Planned",
                "startTime": "2025-01-16T09:00:00",
                "fullPostCodes": ["BT3 3CC"]
            }
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert len(result) == 2
    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] == "2025-01-15T10:00:00"
    assert result[1]["affected_postcodes"] == ["BT3 3CC"]


def test_parse_none_or_empty_data():
    """Test parsing None or empty data returns empty list."""
    assert parse_power_cut_data(None) == []
    assert parse_power_cut_data({}) == []


def test_parse_data_without_outage_message_key():
    """Test parsing data without 'outageMessage' key returns empty list."""
    mock_data = {"otherKey": "value"}

    result = parse_power_cut_data(mock_data)

    assert result == []


def test_parse_outages_with_missing_fields():
    """Test parsing outages with missing fields uses None."""
    mock_data = {
        "outageMessage": [
            {"outageType": "Unplanned"},
            {"startTime": "2025-01-15T11:00:00"}
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] is None
    assert result[1]["outage_date"] == "2025-01-15T11:00:00"


def test_parse_data_includes_correct_provider():
    """Test all entries have correct source provider."""
    mock_data = {
        "outageMessage": [{"outageType": "Planned"}]
    }

    result = parse_power_cut_data(mock_data)

    assert result[0]["source_provider"] == PROVIDER
