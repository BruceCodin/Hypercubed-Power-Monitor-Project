"""Unit tests for SP Northwest power cut extraction functions."""
# pylint: skip-file
# pragma: no cover
from extract_sp_northwest import parse_power_cut_data, PROVIDER


def test_parse_valid_data_with_multiple_items():
    """Test parsing valid data with multiple power cut items."""
    mock_data = {
        "Items": [
            {
                "faultType": "Unplanned",
                "date": "2025-01-15T10:00:00",
                "AffectedPostcodes": ["M1", "M2"]
            },
            {
                "faultType": "Planned",
                "date": "2025-01-16T09:00:00",
                "AffectedPostcodes": ["L1"]
            }
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert len(result) == 2
    assert result[0]["source_provider"] == PROVIDER
    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] == "2025-01-15T10:00:00"
    assert result[0]["affected_postcodes"] == ["M1", "M2"]


def test_parse_none_or_empty_data():
    """Test parsing None or empty data returns empty list."""
    assert parse_power_cut_data(None) == []
    assert parse_power_cut_data({}) == []


def test_parse_data_without_items_key():
    """Test parsing data without 'Items' key returns empty list."""
    mock_data = {"OtherKey": "value"}

    result = parse_power_cut_data(mock_data)

    assert result == []


def test_parse_items_with_missing_fields():
    """Test parsing items with missing fields uses None."""
    mock_data = {
        "Items": [
            {"faultType": "Unplanned"},
            {"date": "2025-01-15T11:00:00"}
        ]
    }

    result = parse_power_cut_data(mock_data)

    assert len(result) == 2
    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] is None
    assert result[1]["status"] is None
    assert result[1]["outage_date"] == "2025-01-15T11:00:00"


def test_parse_data_includes_correct_provider():
    """Test all entries have correct source provider."""
    mock_data = {
        "Items": [
            {"Type": "Planned"},
            {"Type": "Unplanned"}
        ]
    }

    result = parse_power_cut_data(mock_data)

    for entry in result:
        assert entry["source_provider"] == "SP Electricity North West"
