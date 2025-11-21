# pylint: skip-file

"""Unit tests for Northern Powergrid power cut transformation functions."""

from transform_northern_powergrid import (transform_power_cut_data,
                                          transform_postcode,
                                          transform_status)


def test_transform_postcode_lowercase():
    """Test postcode conversion to uppercase."""
    assert transform_postcode("ne1 4st") == ["NE1 4ST"]


def test_transform_postcode_extra_spaces():
    """Test postcode with extra spaces normalized."""
    assert transform_postcode("NE1    4ST") == ["NE1 4ST"]


def test_transform_postcode_mixed_case_and_spaces():
    """Test postcode with mixed case and extra spaces."""
    assert transform_postcode("  dh1   3hp  ") == ["DH1 3HP"]


def test_transform_postcode_already_formatted():
    """Test already formatted postcode remains unchanged."""
    assert transform_postcode("TS1 1AD") == ["TS1 1AD"]


def test_transform_status_planned_work():
    """Test planned work status mapping."""
    assert transform_status("Planned Work on System") == "planned"


def test_transform_status_localised_fault():
    """Test localised fault status mapping."""
    assert transform_status("Localised Fault") == "unplanned"


def test_transform_status_unknown():
    """Test unknown status returns unknown."""
    assert transform_status("Emergency") == "unknown"
    assert transform_status("") == "unknown"


def test_transform_power_cut_data_valid():
    """Test transformation of valid power cut data."""
    data = [
        {
            "recording_time": "2025-01-15T09:00:00",
            "affected_postcodes": "ne1  4st",
            "status": "Planned Work on System",
            "outage_date": "2025-01-15T10:00:00",
            "source_provider": "Northern Powergrid"
        }
    ]
    result = transform_power_cut_data(data)

    assert result[0]["affected_postcodes"] == ["NE1 4ST"]
    assert result[0]["status"] == "planned"
    assert result[0]["outage_date"] == "2025-01-15T10:00:00"


def test_transform_power_cut_data_empty_list():
    """Test transformation of empty list returns None."""
    assert transform_power_cut_data([]) is None


def test_transform_power_cut_data_missing_keys():
    """Test transformation with missing keys skips entry."""
    data = [{"outage_date": "2025-01-15T10:00:00"}]
    result = transform_power_cut_data(data)

    # Entry with missing required keys should remain untransformed
    assert len(result) == 1
    assert result[0] == {"outage_date": "2025-01-15T10:00:00"}


def test_transform_power_cut_data_multiple_entries():
    """Test transformation of multiple power cut entries."""
    data = [
        {
            "recording_time": "2025-01-15T09:00:00",
            "affected_postcodes": "ne1 4st",
            "status": "Localised Fault",
            "outage_date": "2025-01-15T10:00:00",
            "source_provider": "Northern Powergrid"
        },
        {
            "recording_time": "2025-01-15T09:00:00",
            "affected_postcodes": "dh1   3hp",
            "status": "Planned Work on System",
            "outage_date": "2025-01-15T11:00:00",
            "source_provider": "Northern Powergrid"
        }
    ]
    result = transform_power_cut_data(data)

    assert len(result) == 2
    assert result[0]["affected_postcodes"] == ["NE1 4ST"]
    assert result[0]["status"] == "unplanned"
    assert result[1]["affected_postcodes"] == ["DH1 3HP"]
    assert result[1]["status"] == "planned"
