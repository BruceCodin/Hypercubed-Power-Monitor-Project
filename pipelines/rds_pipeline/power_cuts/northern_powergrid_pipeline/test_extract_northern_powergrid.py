# pylint: skip-file

"""Unit tests for Northern Powergrid power cut extraction functions."""

from extract_northern_powergrid import parse_power_cut_data, PROVIDER


def test_parse_valid_data_with_multiple_items():
    """Test parsing valid data with multiple power cuts."""
    mock_data = [
        {
            "NatureOfOutage": "Unplanned",
            "LoggedTime": "2025-01-15T10:00:00",
            "Postcode": "NE1 4ST"
        },
        {
            "NatureOfOutage": "Planned",
            "LoggedTime": "2025-01-16T09:00:00",
            "Postcode": "DH1 3HP"
        }
    ]

    result = parse_power_cut_data(mock_data)

    assert len(result) == 2
    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] == "2025-01-15T10:00:00"
    assert result[1]["affected_postcodes"] == "DH1 3HP"


def test_parse_none_or_empty_data():
    """Test parsing None or empty data returns empty list."""
    assert parse_power_cut_data(None) == []
    assert parse_power_cut_data([]) == []


def test_parse_items_with_missing_fields():
    """Test parsing items with missing fields uses None."""
    mock_data = [
        {"NatureOfOutage": "Unplanned"},
        {"LoggedTime": "2025-01-15T11:00:00"}
    ]

    result = parse_power_cut_data(mock_data)

    assert result[0]["status"] == "Unplanned"
    assert result[0]["outage_date"] is None
    assert result[1]["outage_date"] == "2025-01-15T11:00:00"


def test_parse_data_includes_correct_provider():
    """Test all entries have correct source provider."""
    mock_data = [{"NatureOfOutage": "Planned"}]

    result = parse_power_cut_data(mock_data)

    assert result[0]["source_provider"] == "Northern Powergrid"


def test_parse_single_item():
    """Test parsing single power cut item."""
    mock_data = [
        {
            "NatureOfOutage": "Emergency",
            "LoggedTime": "2025-01-15T12:00:00",
            "Postcode": "TS1 1AD"
        }
    ]

    result = parse_power_cut_data(mock_data)

    assert len(result) == 1
    assert result[0]["source_provider"] == PROVIDER
    assert result[0]["status"] == "Emergency"
    assert result[0]["affected_postcodes"] == "TS1 1AD"
