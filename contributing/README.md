# Contributing to the Energy Monitor Project

This folder contains information and advice for contributions to the project, such as code style, naming conventions, structure, etc.

## Table of Contents
- [Code Style](#code-style)
- [Git Workflow](#git-workflow)
- [Branch Naming](#branch-naming)
- [Commit Messages](#commit-messages)
- [Docstring Style](#docstring-style)
- [Testing](#testing)
- [Pull Requests](#pull-requests)

## Code Style

We follow [PEP 8](https://peps.python.org/pep-0008/) for all Python code.

### Tools
- **Linter:** `pylint` - Run before committing to catch style issues
- **Formatter:** `autopep8` - Auto-formats code to PEP 8 standards

### Running the Tools

```bash
# Format your code with autopep8
autopep8 --in-place --aggressive --aggressive <filename>.py

# Check your code with pylint
pylint <filename>.py

# Run pylint on entire project
pylint src/
```

### Key Style Rules
- **Indentation:** 4 spaces (no tabs)
- **Line length:** Maximum 79 characters
- **Imports:** Group in order: standard library, third-party, local
- **Naming conventions:**
  - `snake_case` for functions and variables
  - `PascalCase` for classes
  - `UPPER_CASE` for constants

### Example
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

## Git Workflow

1. Pull latest changes from `main`
2. Create a new branch for your work
3. Make your changes
4. Run `autopep8` and `pylint`
5. Commit with a descriptive message
6. Push your branch
7. Create a Pull Request

## Branch Naming

**Format:** `type/short-description`

### Types
- `feature/` - New functionality
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code improvements without changing functionality
- `test/` - Adding or updating tests
- `chore/` - Maintenance tasks (dependencies, config, etc.)

### Rules
- All lowercase
- Use hyphens (not underscores or spaces)
- Be descriptive but concise

### Examples
```
feature/extract-pipeline
feature/database-schema
fix/sensor-data-validation
fix/null-temperature-handling
docs/architecture-diagram
docs/erd-update
refactor/transform-error-handling
test/extraction-unit-tests
chore/terraform-rds-setup
chore/docker-compose-config
```

## Commit Messages

**Format:** `type: short summary`

### Rules
- Use imperative mood ("add" not "added" or "adds")
- Maximum 50 characters for the subject line
- No period at the end
- Lowercase after the colon

### Types
Same as branch types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

### Examples
```
feat: add plant data extraction from API
fix: handle null values in soil moisture readings
docs: add ERD diagram to README
refactor: extract validation logic to separate class
test: add unit tests for data cleaning functions
chore: configure Terraform for RDS database
```

### Bad Examples (Don't do this)
```
❌ Fixed the bug
❌ Updated files
❌ WIP
❌ feat: Added the extraction pipeline and also fixed some bugs in the transform
```

## Docstring Style

We use **Google style** docstrings for all functions, classes, and modules.

### Function Docstring Template
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

### Real Example
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

### Class Docstring Example
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

## Testing

- Write tests for all new functionality
- Use `pytest` for testing
- Aim for at least 80% code coverage
- Run tests before committing

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src
```

## Pull Requests

### Before Creating a PR
- [ ] Code follows PEP 8 (run `autopep8` and `pylint`)
- [ ] All tests pass
- [ ] Docstrings added for new functions/classes
- [ ] Updated relevant documentation

### PR Description Template
```markdown
## Description
Brief description of what this PR does

## Type of Change
- [ ] Feature
- [ ] Bug fix
- [ ] Documentation
- [ ] Refactor

## Testing
How was this tested?

## Related Issues
Closes #<issue-number>
```

### Review Process
- At least one team member must review and approve
- Address all comments before merging
- Squash commits if there are many small fixes