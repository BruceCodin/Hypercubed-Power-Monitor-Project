from unittest.mock import patch, MagicMock
from transform import transform_postcode_with_api, transform_postcode_manually


def test_transform_postcode_manually_valid():
    '''Test manual postcode transformation with valid postcodes.'''
    valid_cases = [
        ("BR8 7RE", "BR8 7RE"),
        ("br87re", "BR8 7RE"),
        ("SW1A1AA", "SW1A 1AA"),
    ]
    for input_code, expected in valid_cases:
        assert transform_postcode_manually(input_code) == expected


def test_transform_postcode_manually_invalid():
    '''Test manual postcode transformation with invalid postcodes.'''
    invalid_cases = ["INVALID", 123, None, "BR8 7R", "BR8 7RE1"]
    for input_code in invalid_cases:
        assert transform_postcode_manually(input_code) is None


@patch('transform.requests.get')
def test_api_online_valid(mock_get):
    '''Test API validation when online with valid postcode.'''
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'result': {'postcode': 'BR8 7RE'}}
    mock_get.return_value = mock_response

    assert transform_postcode_with_api("br87re") == "BR8 7RE"


@patch('transform.requests.get')
def test_api_online_invalid(mock_get):
    '''Test API validation when online with invalid postcode (404).'''
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    assert transform_postcode_with_api("INVALID123") is None


@patch('transform.requests.get')
def test_api_offline_fallback_valid(mock_get):
    '''Test fallback to manual validation when API offline (5xx error).'''
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    assert transform_postcode_with_api("BR8 7RE") == "BR8 7RE"


@patch('transform.requests.get')
def test_api_offline_fallback_invalid(mock_get):
    '''Test fallback to manual validation when API offline with invalid postcode.'''
    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_get.return_value = mock_response

    assert transform_postcode_with_api("INVALID") is None
