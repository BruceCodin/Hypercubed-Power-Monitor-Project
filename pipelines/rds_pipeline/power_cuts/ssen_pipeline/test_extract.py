"""Unit tests for SSEN power cut extraction functions."""

from datetime import datetime
from unittest.mock import patch
import pytest
from extract import parse_power_cut_data, PROVIDER


class TestParsePowerCutData:
    """Test suite for parse_power_cut_data function."""

    def test_parse_valid_data_with_multiple_faults(self):
        """Test parsing valid data with multiple faults."""
        mock_data = {
            "Faults": [
                {
                    "py/object": "Restored",
                    "name": "Aberdeen",
                    "loggedAt": "2025-01-15T10:30:00",
                    "affectedAreas": ["AB10", "AB11", "AB12"]
                },
                {
                    "py/object": "Active",
                    "name": "Inverness",
                    "loggedAt": "2025-01-15T11:00:00",
                    "affectedAreas": ["IV1", "IV2"]
                }
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 2
        assert result[0]["source_provider"] == PROVIDER
        assert result[0]["status"] == "Restored"
        assert result[0]["region_affected"] == "Aberdeen"
        assert result[0]["outage_date"] == "2025-01-15T10:30:00"
        assert result[0]["affected_postcodes"] == ["AB10", "AB11", "AB12"]
        assert "recording_time" in result[0]

        assert result[1]["status"] == "Active"
        assert result[1]["region_affected"] == "Inverness"

    def test_parse_valid_data_with_single_fault(self):
        """Test parsing valid data with a single fault."""
        mock_data = {
            "Faults": [
                {
                    "py/object": "Active",
                    "name": "Perth",
                    "loggedAt": "2025-01-15T12:00:00",
                    "affectedAreas": ["PH1", "PH2"]
                }
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 1
        assert result[0]["source_provider"] == PROVIDER
        assert result[0]["status"] == "Active"
        assert result[0]["region_affected"] == "Perth"
        assert result[0]["outage_date"] == "2025-01-15T12:00:00"
        assert result[0]["affected_postcodes"] == ["PH1", "PH2"]

    def test_parse_none_data(self):
        """Test parsing None data returns empty list."""
        result = parse_power_cut_data(None)

        assert result == []
        assert isinstance(result, list)

    def test_parse_empty_dict(self):
        """Test parsing empty dictionary returns empty list."""
        result = parse_power_cut_data({})

        assert result == []
        assert isinstance(result, list)

    def test_parse_data_without_faults_key(self):
        """Test parsing data without 'Faults' key returns empty list."""
        mock_data = {
            "OtherKey": "SomeValue",
            "AnotherKey": 123
        }

        result = parse_power_cut_data(mock_data)

        assert result == []
        assert isinstance(result, list)

    def test_parse_data_with_empty_faults_list(self):
        """Test parsing data with empty faults list."""
        mock_data = {"Faults": []}

        result = parse_power_cut_data(mock_data)

        assert result == []
        assert isinstance(result, list)

    def test_parse_fault_with_missing_fields(self):
        """Test parsing fault entries with missing fields."""
        mock_data = {
            "Faults": [
                {
                    "py/object": "Active",
                    "name": "Glasgow"
                },
                {
                    "loggedAt": "2025-01-15T13:00:00",
                    "affectedAreas": ["G1"]
                }
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 2
        assert result[0]["status"] == "Active"
        assert result[0]["region_affected"] == "Glasgow"
        assert result[0]["outage_date"] is None
        assert result[0]["affected_postcodes"] is None

        assert result[1]["status"] is None
        assert result[1]["region_affected"] is None
        assert result[1]["outage_date"] == "2025-01-15T13:00:00"
        assert result[1]["affected_postcodes"] == ["G1"]

    def test_parse_data_includes_source_provider(self):
        """Test that all parsed entries include correct source provider."""
        mock_data = {
            "Faults": [
                {"py/object": "Active", "name": "Edinburgh"},
                {"py/object": "Restored", "name": "Dundee"}
            ]
        }

        result = parse_power_cut_data(mock_data)

        for entry in result:
            assert entry["source_provider"] == PROVIDER
            assert (entry["source_provider"] ==
                    "Scottish and Southern Electricity Networks")

    @patch('extract.datetime')
    def test_parse_data_includes_recording_time(self, mock_datetime):
        """Test that recording_time is set to current time."""
        fixed_time = datetime(2025, 1, 15, 14, 30, 0)
        mock_datetime.now.return_value = fixed_time

        mock_data = {
            "Faults": [
                {"py/object": "Active", "name": "Stirling"}
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 1
        assert result[0]["recording_time"] == fixed_time.isoformat()

    def test_parse_data_structure(self):
        """Test that parsed data has correct structure."""
        mock_data = {
            "Faults": [
                {
                    "py/object": "Active",
                    "name": "Fort William",
                    "loggedAt": "2025-01-15T15:00:00",
                    "affectedAreas": ["PH33"]
                }
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 1
        entry = result[0]

        required_keys = [
            "source_provider",
            "status",
            "region_affected",
            "outage_date",
            "recording_time",
            "affected_postcodes"
        ]

        for key in required_keys:
            assert key in entry, f"Missing key: {key}"

    def test_parse_data_with_extra_fields_ignored(self):
        """Test that extra fields in fault data are ignored."""
        mock_data = {
            "Faults": [
                {
                    "py/object": "Active",
                    "name": "Oban",
                    "loggedAt": "2025-01-15T16:00:00",
                    "affectedAreas": ["PA34"],
                    "extraField1": "ignored",
                    "extraField2": 999
                }
            ]
        }

        result = parse_power_cut_data(mock_data)

        assert len(result) == 1
        assert "extraField1" not in result[0]
        assert "extraField2" not in result[0]
        assert result[0]["status"] == "Active"
        assert result[0]["region_affected"] == "Oban"
