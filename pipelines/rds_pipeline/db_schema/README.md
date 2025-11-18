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

Power outage notification system.

### Tables
- **DIM_customer** - Customer information
- **FACT_outage** - Power outage events
- **BRIDGE_affected_postcodes** - Postcodes affected by outages
- **BRIDGE_subscribed_postcodes** - Customer postcode subscriptions

## Setup
power-monitor-db is the name of the RDS database which the two schemas will uploaded to.

```bash
# Run both schemas
psql -h <rds-endpoint> -U admin -d power-monitor-db -f power_generation_schema.sql
psql -h <rds-endpoint> -U admin -d power-monitor-db -f subscriber_alerts_schema.sql
```

## Testing

Test the power generation schema before deployment:

```bash
# 1. Create the schema
psql -d power-monitor-db -f power_generation_schema.sql

# 2. Run tests
psql -d power-monitor-db -f test_power_generation_schema.sql
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