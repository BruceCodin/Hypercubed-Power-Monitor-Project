# Code Style

We follow [PEP 8](https://peps.python.org/pep-0008/) for all Python code.

## Tools
- **Linter:** `pylint` - Run before committing to catch style issues
- **Formatter:** `autopep8` - Auto-formats code to PEP 8 standards

## Running the Tools

```bash
# Format your code with autopep8
autopep8 --in-place --aggressive --aggressive <filename>.py

# Check your code with pylint
pylint <filename>.py

# Run pylint on entire project
pylint src/
```

## Key Style Rules
- **Indentation:** 4 spaces (no tabs)
- **Line length:** Maximum 79 characters
- **Imports:** Group in order: standard library, third-party, local
- **Naming conventions:**
  - `snake_case` for functions and variables
  - `PascalCase` for classes
  - `UPPER_CASE` for constants

## Example
```python
# Good
def fetch_plant_data(plant_id):
    """Fetch sensor data for a plant."""
    api_url = f"https://sigma-labs-bot.herokuapp.com/api/plants/{plant_id}"
    return requests.get(api_url)

# Bad
def FetchPlantData(plantId):
    apiURL = f"https://sigma-labs-bot.herokuapp.com/api/plants/{plantId}"
    return requests.get(apiURL)
```
