"""Unit tests for SSEN power cut extraction functions."""
# pylint: skip-file
from extract_ssen import parse_power_cut_data, PROVIDER


def test_parse_valid_data_with_multiple_faults():
    """Test parsing valid data with multiple faults."""
    mock_data = {
        "Faults": [
            {
                "py/object": "Restored",
                "name": "Aberdeen",
                "loggedAt": "2025-01-15T10:30:00",
                "affectedAreas": ["AB10", "AB11"]
            },
            {
                "py/object": "Active",
                "name": "Inverness",
                "loggedAt": "2025-01-15T11:00:00",
                "affectedAreas": ["IV1"]
            }
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert len(result) == 2
    assert result[0]["status"] == "Restored"
    assert result[0]["region_affected"] == "Aberdeen"
    assert result[1]["affected_postcodes"] == ["IV1"]


def test_parse_none_or_empty_data():
    """Test parsing None or empty data returns empty list."""
    assert parse_power_cut_data(None) == []
    assert parse_power_cut_data({}) == []
    assert parse_power_cut_data({"Faults": []}) == []


def test_parse_data_without_faults_key():
    """Test parsing data without 'Faults' key returns empty list."""
    mock_data = {"OtherKey": "value"}

    result = parse_power_cut_data(mock_data)

    assert result == []


def test_parse_fault_with_missing_fields():
    """Test parsing fault entries with missing fields."""
    mock_data = {
        "Faults": [
            {"py/object": "Active", "name": "Glasgow"},
            {"loggedAt": "2025-01-15T13:00:00"}
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert result[0]["status"] == "Active"
    assert result[0]["outage_date"] is None
    assert result[1]["region_affected"] is None


def test_parse_data_includes_correct_provider():
    """Test all entries have correct source provider."""
    mock_data = {
        "Faults": [{"py/object": "Active", "name": "Edinburgh"}]
    }

    result = parse_power_cut_data(mock_data)

    assert result[0]["source_provider"] == PROVIDER
