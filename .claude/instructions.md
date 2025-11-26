# Energy Monitor Project - Claude Code Instructions

## Project Context
You are working on the Energy Monitor Project - a cloud-based ETL pipeline that tracks energy generation/costs, providing users with a dashboard for a clear, understandable overview of power usage in the UK and receive summaries/alerts by SES/SNS.

**Key Facts:**
- 4-person team project with GitHub collaboration
- Tech stack: Python (requests, Pandas, Psycopg2), SQL, Docker, Terraform, AWS (RDS, ECS, SNS/SES)
- Data collection: varies between different API endpoints, e.g. every 5 minutes or hourly
- Storage: <24hr in RDS, >24hr summaries in S3
- Emphasis on: OOP, TDD, clean code, cost optimization
- Project deadline: Friday 5pm

## Coding Standards & Principles

### 1. Code Quality
- **Always write clean, readable code** - L prefers simplicity over complexity
- **Pylint score must be >8.0** - run `pylint` before committing
- **Follow PEP 8** - use descriptive variable names, proper spacing
- **Add type hints** - all function signatures should have type annotations
- **Docstrings required** - all classes and functions need docstrings (Google style)
- **No overly complex solutions** - if it feels complicated, simplify it

#### Coding Preferences
- Follow existing project style guides and formatting conventions.
- Add brief, high-value comments only when intent is non-obvious.
- Avoid introducing new dependencies unless explicitly requested.
- When editing, minimize the diff to the essential change set.
- Reference touched files with `path:line` so they are easy to locate.

#### Communication Workflow
- Clarify ambiguous requirements before coding when possible.
- Explain decisions: what changed, where, and why it matters.
- Surface risks, trade-offs, or follow-up ideas proactively.
- Suggest logical next steps (tests to run, commits, reviews) only when they add value.

#### Checkins

- After reading code, after each iteration of edits.

### 2. Object-Oriented Programming (OOP)
- Use classes for major components (API client, validators, database managers)
- Single Responsibility Principle - each class does one thing well
- Avoid God classes - break down large classes into smaller ones
- Use inheritance/composition where appropriate
- The CTO loves OOP - make it count

### 3. Test-Driven Development (TDD)
- **Write tests FIRST** before implementation code
- Target: >80% code coverage
- Use pytest framework
- Structure: Arrange-Act-Assert (AAA) pattern
- Mock external dependencies (API calls, database, S3)
- Use fixtures for reusable test data
- The CTO is a big fan of TDD - follow this practice



### 4. Git & GitHub Workflow
**CRITICAL - This is a 4-person team project:**

#### When Reviewing Others' PRs
- Check code quality and style
- Run code locally if possible
- Verify tests pass
- Provide constructive feedback
- Approve if all looks good

### 5. Environment & Configuration
- **Never commit secrets** - use `.env` file (add to `.gitignore`)
- **Use environment variables** for all configuration
- **Provide `.env.example`** with dummy values for team
- **AWS credentials** - use AWS CLI profiles or IAM roles (never hardcode)
- **Database credentials** - from environment variables only

### 6. Error Handling & Logging
- Use Python's `logging` module (not `print` statements)
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Catch specific exceptions (not bare `except:`)
- Always log errors with context (plant_id, timestamp, error message)
- Use try-except-finally for resource cleanup

### 7. Dependencies & Requirements
- Add all new dependencies to `requirements.txt`
- Pin versions for reproducibility: `requests==2.31.0` (not `requests`)
- Document why major dependencies are needed
- Keep dependencies minimal

### 8. Documentation
- Add clear docstrings to all functions and classes
- Update README.md as features are added
- Document setup steps clearly
- Include example commands
- Explain architectural decisions

## API Specifics

### Elexon Insights APIs
- Base URL: `https://data.elexon.co.uk/bmrs/api/v1/`
- Data: generation mix (by fuel type) and system prices
- Update Frequency: 30-minute settlement periods (48 periods per day)
- Method: GET
- Authentication: None
- Endpoints:
  - Generation: `/generation/outturn/summary` - shows distribution of fuel types
  - Pricing: `/balancing/settlement/system-prices/{settlementDate}` - system buy/sell prices

**Generation Response:**
```json
[
  {
    "startTime": "2025-11-16T10:55:00Z",
    "settlementPeriod": 22,
    "data": [
      {
        "fuelType": "BIOMASS",
        "generation": 3322
      },
      {
        "fuelType": "CCGT",
        "generation": 12247
      },
      {
        "fuelType": "WIND",
        "generation": 6217
      }
    ]
  }
]
```

**System Prices Response:**
```json
{
  "dataset": "SYSPRICE",
  "settlementDate": "2024-11-17",
  "settlementPeriod": 25,
  "systemSellPrice": 85.50,
  "systemBuyPrice": 82.30,
  "priceDerivationCode": "NORMAL",
  "reserveScarcityPrice": 0.00,
  "netImbalanceVolume": 152.5
}
```

**Key Parameters:**
- `settlementDate` - YYYY-MM-DD format
- `settlementPeriod` - 1-48 (each period = 30 minutes)
- `from` / `to` - Date range queries
- `format` - json or csv (defaults to json)

### Carbon Intensity API
- Base URL: `https://api.carbonintensity.org.uk/`
- Data: carbon intensity forecasts and actual measurements (gCO2/kWh)
- Update Frequency: 30-minute intervals
- Method: GET
- Authentication: None
- Endpoints:
  - Current: `/intensity` - latest carbon intensity with traffic light index
  - Range: `/intensity/{from}/{to}` - historical data (ISO 8601 timestamps)

**Current Intensity Response:**
```json
{
  "data": [
    {
      "from": "2025-11-17T10:30Z",
      "to": "2025-11-17T11:00Z",
      "intensity": {
        "forecast": 122,
        "actual": 118,
        "index": "moderate"
      }
    }
  ]
}
```

**Range Query Response:**
```json
{
  "data": [
    {
      "from": "2024-11-16T00:00Z",
      "to": "2024-11-16T00:30Z",
      "intensity": {
        "forecast": 245,
        "actual": 242,
        "index": "high"
      }
    },
    {
      "from": "2024-11-16T00:30Z",
      "to": "2024-11-16T01:00Z",
      "intensity": {
        "forecast": 238,
        "actual": 235,
        "index": "high"
      }
    }
  ]
}
```

**Key Parameters:**
- `from` / `to` - ISO 8601 timestamps (e.g., `2024-11-16T00:00Z`)
- Carbon intensity index: "very low", "low", "moderate", "high", "very high"
- `actual` may be null for future periods (forecasts only)

### National Grid ESO (NESO) Data Portal API
- Base URL: `https://api.neso.energy/api/3/action/`
- Data: demand, generation, interconnector flows, embedded generation
- Update Frequency: 30-minute settlement periods
- Method: GET (SQL queries via datastore_search_sql)
- Authentication: None (public API)
- Endpoint: `datastore_search_sql` - SQL-based query interface

**Query Example:**
```
GET https://api.neso.energy/api/3/action/datastore_search_sql?sql=
SELECT * FROM "{resource_id}"
WHERE "SETTLEMENT_DATE" = '2024-11-17'
ORDER BY "SETTLEMENT_PERIOD"
LIMIT 48
```

**Response Format:**
```json
{
  "success": true,
  "result": {
    "records": [
      {
        "SETTLEMENT_DATE": "2024-11-17",
        "SETTLEMENT_PERIOD": 1,
        "ND": 38542.5,
        "TSD": 36234.2,
        "EMBEDDED_WIND_GENERATION": 2145,
        "EMBEDDED_SOLAR_GENERATION": 156,
        "IFA_FLOW": 1250,
        "BRITNED_FLOW": 850,
        "MOYLE_FLOW": 450
      }
    ],
    "fields": [],
    "sql": ""
  }
}
```

**Key Datasets & Columns:**
- `ND` - National Demand (MW)
- `TSD` - Transmission System Demand (MW)
- `EMBEDDED_WIND_GENERATION` - Embedded wind generation (MW)
- `EMBEDDED_SOLAR_GENERATION` - Embedded solar generation (MW)
- `PUMP_STORAGE_PUMPING` - Pump storage pumping (MW)
- Interconnector flows: `IFA_FLOW`, `BRITNED_FLOW`, `MOYLE_FLOW`, `GREENLINK_FLOW`, `VIKING_FLOW`, `NEMO_FLOW`, `ELECLINK_FLOW`, `NSL_FLOW`, `EAST_WEST_FLOW`
- `SCOTLAND_TO_ENGLAND_TRANSFER` - Cross-border flow (MW)

## Database Schema Guidelines

### Expected Tables (3NF Normalized)

**DIM_customer**
- customer_id (int, generated, primary key)
- first_name (text, not null)
- last_name (text, not null)
- email (text, unique, not null)
format constraint: email like `%_@__%.__%`

**FACT_outage**
- outage_id (int, generated, primary key)
- source_provider (text, not null)
- status (text)
- outage_date (date)
- recording_time (timestamp, default now)

**FACT_notification_log**
- notification_id (serial, primary key)
- customer_id (int, foreign key (DIM_customer))
- outage_id (int, foreign key (FACT_outage))
- sent_at (timestamp, default now)
uniqueness constraint: (customer_id, outage_id)

**BRIDGE_affected_postcodes**
- affected_id (int, generated, primary key)
- outage_id (int, not null, foreign key (FACT_outage))
- postcode_affected (text, not null)

**BRIDGE_subscribed_postcodes**
- subscription_id (int, generated, primary key)
- customer_id (int, not null, foreign key (DIM_customer))
- postcode (text, not null)

## AWS Services & Architecture

### Required AWS Services
- **SQL Server (RDS)** - Short-term storage (<24hr)
- **S3** - Long-term archive (>24hr summaries) - cost-effective storage
- **ECS (Fargate)** - Container orchestration for running dashboard
- **Lambda** - run pipeline and other repetitive tasks
- **ECR** - Container registry for Docker images
- **EventBridge** - Scheduling (every minute for extraction, daily for archival)
- **IAM** - Roles and permissions
- **CloudWatch** - Logging and monitoring

### Terraform Best Practices
- Use modules for reusable components
- Variable naming: lowercase with underscores
- Add descriptions to all variables
- Output important values (ARNs, URLs, endpoints)
- Tag all resources: `Project = "Power Monitor"`, `Environment = "dev/prod"`, `ManagedBy = "Terraform"`
- Use remote state with S3 backend
- Run `terraform fmt` before committing
- Never commit `.tfstate` files or `.terraform` directories
- Add to `.gitignore`:
```
  .terraform/
  *.tfstate
  *.tfstate.backup
  .terraform.lock.hcl
```

### Docker Best Practices
- Use official Python slim images (e.g., `python:3.11-slim`)
- Multi-stage builds if needed for smaller images
- Target image size: <500MB
- Run as non-root user for security
- Use `.dockerignore` to exclude:
```
  .git/
  .github/
  tests/
  *.pyc
  __pycache__/
  .env
  .pytest_cache/
```

## Project Workflow

### Planned Outputs
- An interactive dashboard that allows users to track & analyse power generation (e.g. by source), carbon intensity, and price both in near real-time and historically
- Regular summary reports for subscribed users on power generation/price/carbon
- Subscribable alerts for power outages in specific postcode regions.
- Auto-generated human-understandable summaries of key data stories

## Common Pitfalls to Avoid

1. **Don't hardcode credentials** - always use environment variables
2. **Don't commit secrets** - use `.env` and `.gitignore`
3. **Don't merge your own PRs** - always get team review
4. **Don't skip tests** - TDD is emphasized by the CTO
5. **Don't over-engineer** - L prefers simple, working solutions
6. **Don't forget to pull before starting work** - avoid merge conflicts
7. **Don't use bare except clauses** - catch specific exceptions
8. **Don't use print()** - use logging module
9. **Don't forget type hints and docstrings** - code quality matters
10. **Don't work directly on main** - always use feature branches

## When Writing Code

### Always Consider:
- ✅ Is this the simplest solution?
- ✅ Have I written tests first?
- ✅ Are there type hints and docstrings?
- ✅ Will this pass pylint (>8.0)?
- ✅ Have I logged errors appropriately?
- ✅ Are secrets in environment variables?
- ✅ Will my teammates understand this code?
- ✅ Have I pulled latest changes from main?

### Deliverables Checklist (by Friday 5pm)
- [ ] GitHub repository (public or collaborators invited)
- [ ] README.md with setup instructions and project explanation
- [ ] ERD diagram
- [ ] Architecture diagram
- [ ] Working ETL pipeline deployed on AWS
- [ ] Tests with >80% coverage
- [ ] All code reviewed and merged
- [ ] GitHub Projects board showing completed work
- [ ] Links to deployed resources (dashboards if applicable)
- [ ] Short video demo

## Communication

- Use GitHub Projects for ticket tracking
- Use GitHub Issues for bugs and discussions
- Use PR comments for code-specific discussions
- Regular standups with team (coordinate timing)
- Don't hesitate to ask Senior Engineer for architectural guidance
- Keep team informed of blockers