"""Unit tests for SSEN power cut transformation functions."""
# pylint: skip-file
# pragma: no cover

from transform_ssen import (
    transform_status,
    transform_ssen_data
)


def test_transform_status_psi_to_planned():
    """Test PSI status transforms to planned."""
    assert transform_status("PSI") == "planned"


def test_transform_status_other_to_unplanned():
    """Test non-PSI statuses transform to unplanned."""
    assert transform_status("Active") == "unplanned"
    assert transform_status("Restored") == "unplanned"
    assert transform_status("") == "unplanned"


def test_transform_ssen_data_with_valid_data():
    """Test transformation with valid data."""
    data = [
        {
            "recording_time": "2025-01-15T10:30:00",
            "outage_date": "2025-01-15T10:00:00",
            "affected_postcodes": ["AB10"],
            "status": "PSI",
            "source_provider": "SSEN"
        }
    ]
    result = transform_ssen_data(data)
    assert result[0]["status"] == "planned"


def test_transform_ssen_data_with_empty_data():
    """Test transformation with empty data returns None."""
    result = transform_ssen_data([])
    assert result is None


def test_transform_ssen_data_with_missing_keys():
    """Test transformation skips entries with missing keys."""
    data = [
        {"recording_time": "2025-01-15T10:30:00"},
        {
            "recording_time": "2025-01-15T10:30:00",
            "outage_date": "2025-01-15T10:00:00",
            "affected_postcodes": ["AB10"],
            "status": "PSI",
            "source_provider": "SSEN"
        }
    ]
    result = transform_ssen_data(data)
    assert len(result) == 2


def test_transform_ssen_data_with_invalid_date():
    """Test transformation handles invalid date format."""
    data = [
        {
            "recording_time": "2025-01-15T10:30:00",
            "outage_date": "invalid-date",
            "affected_postcodes": ["AB10"],
            "status": "Active",
            "source_provider": "SSEN"
        }
    ]
    result = transform_ssen_data(data)
    assert result[0]["status"] == "unplanned"


def test_transform_ssen_data_validates_all_columns():
    """Test that all expected columns are validated."""
    data = [
        {
            "recording_time": "2025-01-15T10:30:00",
            "outage_date": "2025-01-15T10:00:00",
            "affected_postcodes": ["AB10"],
            "status": "PSI",
            "source_provider": "SSEN"
        }
    ]
    result = transform_ssen_data(data)
    assert all(key in result[0] for key in [
        "recording_time", "outage_date", "affected_postcodes",
        "status", "source_provider"
    ])
