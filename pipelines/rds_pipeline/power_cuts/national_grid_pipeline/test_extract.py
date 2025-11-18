# test_extract.py
import pytest
from datetime import datetime
from extract import (
    fetch_raw_data,
    parse_records,
    validate_record,
    transform_record,
    extract_power_cuts
)

BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action/datastore_search"
RESOURCE_ID = "292f788f-4339-455b-8cc0-153e14509d4d"


"""Test API Response Handling"""


def test_fetch_raw_data_success(requests_mock):
    """Test fetching power cuts data returns dictionary."""
    # Arrange
    mock_response = {
        "success": True,
        "result": {
            "total": 2,
            "records": [
                {
                    "Postcode": "EX37 9TB",
                    "Start Time": "2025-11-14T15:33:00",
                    "Status": "In Progress"
                }
            ]
        }
    }
    requests_mock.get(
        f"{BASE_URL}?resource_id={RESOURCE_ID}&limit=1000",
        json=mock_response,
        status_code=200
    )

    # Act
    result = fetch_raw_data()

    # Assert
    assert result is not None
    assert result["success"] is True


def test_fetch_raw_data_server_error(requests_mock):
    """Test server error returns None."""
    # Arrange
    requests_mock.get(
        f"{BASE_URL}?resource_id={RESOURCE_ID}&limit=1000",
        status_code=500
    )

    # Act
    result = fetch_raw_data()

    # Assert
    assert result is None


"""Test Data Parsing"""


def test_parse_records_extracts_records():
    """Test parsing records from API response."""
    # Arrange
    raw_data = {
        "success": True,
        "result": {
            "total": 2,
            "records": [
                {"Postcode": "EX37 9TB"},
                {"Postcode": "BS20 6NB"}
            ]
        }
    }

    # Act
    result = parse_records(raw_data)

    # Assert
    assert len(result) == 2


def test_parse_records_empty_response():
    """Test parsing empty response returns empty list."""
    # Arrange
    raw_data = {
        "success": True,
        "result": {
            "total": 0,
            "records": []
        }
    }

    # Act
    result = parse_records(raw_data)

    # Assert
    assert result == []


"""Test Record Validation"""


def test_validate_record_with_all_required_fields():
    """Test valid record with all required fields returns True."""
    # Arrange
    record = {
        "Postcode": "EX37 9TB",
        "Start Time": "2025-11-14T15:33:00",
        "Status": "In Progress"
    }

    # Act
    result = validate_record(record)

    # Assert
    assert result is True


@pytest.mark.parametrize("record,expected", [
    # Missing postcode
    ({"Start Time": "2025-11-14T15:33:00", "Status": "In Progress"}, False),
    # Empty postcode
    ({"Postcode": "", "Start Time": "2025-11-14T15:33:00", "Status": "In Progress"}, False),
    # None postcode
    ({"Postcode": None, "Start Time": "2025-11-14T15:33:00",
     "Status": "In Progress"}, False),
    # Missing start time
    ({"Postcode": "EX37 9TB", "Status": "In Progress"}, False),
    # Empty start time
    ({"Postcode": "EX37 9TB", "Start Time": "", "Status": "In Progress"}, False),
])
def test_validate_record_invalid_cases(record, expected):
    """Test record validation for various invalid cases."""
    # Act
    result = validate_record(record)

    # Assert
    assert result == expected


"""Test Record Transformation"""


@pytest.mark.parametrize("postcode,expected_postcode", [
    ("EX37 9TB", "EX37 9TB"),
    ("  EX37 9TB  ", "EX37 9TB"),
    ("BS20 6NB", "BS20 6NB"),
])
def test_transform_record_postcode_handling(postcode, expected_postcode):
    """Test transformation handles postcodes correctly."""
    # Arrange
    record = {
        "Postcode": postcode,
        "Start Time": "2025-11-14T15:33:00",
        "Status": "In Progress"
    }

    # Act
    result = transform_record(record)

    # Assert
    assert result["postcode"] == expected_postcode


@pytest.mark.parametrize("start_time_str,expected_year,expected_month,expected_day", [
    ("2025-11-14T15:33:00", 2025, 11, 14),
    ("2025-10-22T08:59:00", 2025, 10, 22),
    ("2025-11-15T00:00:00", 2025, 11, 15),
])
def test_transform_record_datetime_conversion(start_time_str, expected_year, expected_month, expected_day):
    """Test transformation converts start time to datetime correctly."""
    # Arrange
    record = {
        "Postcode": "EX37 9TB",
        "Start Time": start_time_str,
        "Status": "In Progress"
    }

    # Act
    result = transform_record(record)

    # Assert
    assert isinstance(result["start_time"], datetime)
    assert result["start_time"].year == expected_year
    assert result["start_time"].month == expected_month
    assert result["start_time"].day == expected_day


def test_transform_record_adds_metadata():
    """Test transformation adds required metadata fields."""
    # Arrange
    record = {
        "Postcode": "EX37 9TB",
        "Start Time": "2025-11-14T15:33:00",
        "Status": "In Progress"
    }

    # Act
    result = transform_record(record)

    # Assert
    assert result["data_source"] == "national_grid"
    assert "extracted_at" in result
    assert isinstance(result["extracted_at"], datetime)


"""Test Full Extraction Pipeline"""


def test_extract_power_cuts_returns_list(requests_mock):
    """Test full extraction returns list of power cuts."""
    # Arrange
    mock_response = {
        "success": True,
        "result": {
            "total": 2,
            "records": [
                {
                    "Postcode": "EX37 9TB",
                    "Start Time": "2025-11-14T15:33:00",
                    "Status": "In Progress"
                },
                {
                    "Postcode": "BS20 6NB",
                    "Start Time": "2025-11-14T16:00:00",
                    "Status": "Awaiting"
                }
            ]
        }
    }
    requests_mock.get(
        f"{BASE_URL}?resource_id={RESOURCE_ID}&limit=1000",
        json=mock_response,
        status_code=200
    )

    # Act
    result = extract_power_cuts()

    # Assert
    assert isinstance(result, list)
    assert len(result) == 2


def test_extract_power_cuts_filters_invalid_records(requests_mock):
    """Test extraction filters out records with missing postcodes."""
    # Arrange
    mock_response = {
        "success": True,
        "result": {
            "total": 3,
            "records": [
                {
                    "Postcode": "EX37 9TB",
                    "Start Time": "2025-11-14T15:33:00",
                    "Status": "In Progress"
                },
                {
                    "Postcode": "",
                    "Start Time": "2025-11-14T16:00:00",
                    "Status": "Awaiting"
                },
                {
                    "Postcode": "BS20 6NB",
                    "Start Time": "2025-11-14T17:00:00",
                    "Status": "In Progress"
                }
            ]
        }
    }
    requests_mock.get(
        f"{BASE_URL}?resource_id={RESOURCE_ID}&limit=1000",
        json=mock_response,
        status_code=200
    )

    # Act
    result = extract_power_cuts()

    # Assert
    assert len(result) == 2
