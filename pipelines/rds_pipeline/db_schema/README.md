# Database Schemas

Two PostgreSQL schemas for energy data management.

## 1. Settlement Schema (`power_generation_schema.sql`)

Energy market settlement data tracking.

### Tables
- **settlements** - Settlement periods (48 per day)
- **system_price** - Market pricing data
- **carbon_intensity** - Carbon emissions data
- **fuel_type** - Power generation fuel types
- **generation** - Power generation by fuel type
- **recent_demand** - Recent demand metrics
- **historic_demand** - Historical demand metrics

## 2. Outage Schema (`subscriber_alerts_schema.sql`)

Customer outage notification system.

### Tables
- **DIM_customer** - Customer information
- **FACT_outage** - Power outage events
- **BRIDGE_affected_postcodes** - Postcodes affected by outages
- **BRIDGE_subscribed_postcodes** - Customer postcode subscriptions

## Setup
```bash
# Run both schemas
psql -d power_monitor_db -f power_generation_schema.sql
psql -d power_monitor_db -f subscriber_alerts_schema.sql
```

## Testing

Test the power generation schema before deploying:

```bash
# 1. Create the schema
psql -d your_database -f power_generation_schema.sql

# 2. Run tests
psql -d your_database -f test_power_generation_schema.sql
```

The test file validates:
- Constraint checks (settlement periods 1-48)
- Foreign key relationships
- Unique constraints
- CASCADE deletes

## Notes

- Settlement periods validated between 1-48
- Customer emails must be unique
- Foreign keys maintain referential integrity