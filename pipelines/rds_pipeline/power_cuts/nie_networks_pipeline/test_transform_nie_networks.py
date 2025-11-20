"""Unit tests for NIE Networks power cut transformation functions."""

from datetime import datetime
from transform import (transform_power_cut_data,
                       transform_postcode,
                       transform_status,
                       transform_outage_date)


def test_transform_postcode_single_lowercase():
    """Test single postcode conversion to uppercase."""
    assert transform_postcode("bt1 1aa") == ["BT1 1AA"]


def test_transform_postcode_multiple_semicolon():
    """Test multiple postcodes separated by semicolon."""
    result = transform_postcode("bt1 1aa;bt2 2bb;bt3 3cc")
    assert result == ["BT1 1AA", "BT2 2BB", "BT3 3CC"]


def test_transform_postcode_extra_spaces():
    """Test postcode with extra spaces normalized."""
    assert transform_postcode("BT1    1AA") == ["BT1 1AA"]


def test_transform_postcode_empty_string():
    """Test empty string returns empty list."""
    assert transform_postcode("") == []


def test_transform_status_fault_keyword():
    """Test status with fault keyword returns unplanned."""
    assert transform_status("Network Fault") == "unplanned"
    assert transform_status("FAULT DETECTED") == "unplanned"


def test_transform_status_planned_keyword():
    """Test status with planned keyword returns planned."""
    assert transform_status("Planned Maintenance") == "planned"
    assert transform_status("PLANNED WORK") == "planned"


def test_transform_status_unknown():
    """Test unknown status returns unknown."""
    assert transform_status("Emergency") == "unknown"
    assert transform_status("") == "unknown"


def test_transform_outage_date_valid_format():
    """Test valid date format converts to ISO format."""
    result = transform_outage_date("10:30 AM, 15 Jan")
    expected_year = datetime.now().year
    assert result == f"{expected_year}-01-15T10:30:00"


def test_transform_outage_date_pm_format():
    """Test PM time format converts correctly."""
    result = transform_outage_date("03:45 PM, 20 Mar")
    expected_year = datetime.now().year
    assert result == f"{expected_year}-03-20T15:45:00"


def test_transform_power_cut_data_valid():
    """Test transformation of valid power cut data."""
    data = [{
        "affected_postcodes": "bt1  1aa;bt2 2bb",
        "status": "Network Fault",
        "outage_date": "10:30 AM, 15 Jan"
    }]
    result = transform_power_cut_data(data)

    assert result[0]["affected_postcodes"] == ["BT1 1AA", "BT2 2BB"]
    assert result[0]["status"] == "unplanned"
    assert "01-15T10:30:00" in result[0]["outage_date"]


def test_transform_power_cut_data_empty_list():
    """Test transformation of empty list returns None."""
    assert transform_power_cut_data([]) is None


def test_transform_power_cut_data_multiple_entries():
    """Test transformation of multiple power cut entries."""
    data = [
        {"affected_postcodes": "bt1 1aa", "status": "Fault",
         "outage_date": "10:00 AM, 15 Jan"},
        {"affected_postcodes": "bt2 2bb", "status": "Planned Work",
         "outage_date": "02:00 PM, 16 Jan"}
    ]
    result = transform_power_cut_data(data)

    assert len(result) == 2
    assert result[0]["affected_postcodes"] == ["BT1 1AA"]
    assert result[0]["status"] == "unplanned"
    assert result[1]["affected_postcodes"] == ["BT2 2BB"]
    assert result[1]["status"] == "planned"
