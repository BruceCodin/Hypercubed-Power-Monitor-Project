# Docstring Style

We use **Google style** docstrings for all functions, classes, and modules.

## Function Docstring Template
```python
def function_name(param1, param2):
    """Short description of what the function does.
    
    Longer description if needed, explaining the purpose
    and any important details.
    
    Args:
        param1: Description of first parameter
        param2: Description of second parameter
        
    Returns:
        Description of return value
        
    Raises:
        ExceptionType: When and why this exception is raised
    """
```

## Real Example
```python
def extract_plant_data(plant_id):
    """Fetches sensor data for a specific plant from the API.
    
    Retrieves temperature, soil moisture, and other sensor readings
    for the specified plant. Handles API errors gracefully.
    
    Args:
        plant_id: The unique identifier for the plant (1-50)
        
    Returns:
        Dictionary containing plant sensor readings with keys:
        - temperature: float
        - soil_moisture: float
        - timestamp: datetime
        
    Raises:
        APIError: If the API request fails or returns invalid data
        ValueError: If plant_id is not in valid range (1-50)
    """
```

## Class Docstring Example
```python
class PlantDataExtractor:
    """Handles extraction of plant sensor data from the API.
    
    This class manages API connections and data retrieval for all
    plants in the LNHM conservatory.
    
    Attributes:
        base_url: The API endpoint base URL
        timeout: Request timeout in seconds
    """
```
