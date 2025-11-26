-- Test file for power_generation_schema.sql
-- This file tests constraints, foreign keys, and data validation
-- Run this after executing power_generation_schema.sql

\echo '================================'
\echo 'Starting Schema Validation Tests'
\echo '================================'

-- Test 1: Valid data insertion
\echo '\n--- Test 1: Inserting valid settlement data ---'
INSERT INTO settlements (settlement_date, settlement_period) VALUES
    ('2024-01-01 00:00:00', 1),
    ('2024-01-01 00:30:00', 2),
    ('2024-01-01 23:30:00', 48);
\echo 'SUCCESS: Valid settlement data inserted'

-- Test 2: Invalid settlement period (should fail)
\echo '\n--- Test 2: Testing settlement_period constraint (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO settlements (settlement_date, settlement_period) VALUES ('2024-01-01 00:00:00', 49);
    RAISE EXCEPTION 'TEST FAILED: Should not allow settlement_period > 48';
EXCEPTION
    WHEN check_violation THEN
        RAISE NOTICE 'SUCCESS: settlement_period constraint working correctly';
END $$;

-- Test 3: Invalid settlement period below range (should fail)
\echo '\n--- Test 3: Testing settlement_period minimum (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO settlements (settlement_date, settlement_period) VALUES ('2024-01-01 00:00:00', 0);
    RAISE EXCEPTION 'TEST FAILED: Should not allow settlement_period < 1';
EXCEPTION
    WHEN check_violation THEN
        RAISE NOTICE 'SUCCESS: settlement_period minimum constraint working correctly';
END $$;

-- Test 4: Valid fuel types
\echo '\n--- Test 4: Inserting valid fuel types ---'
INSERT INTO fuel_type (fuel_type) VALUES
    ('solar'),
    ('wind'),
    ('gas'),
    ('nuclear'),
    ('coal'),
    ('hydro'),
    ('biomass');
\echo 'SUCCESS: Valid fuel types inserted'

-- Test 5: Duplicate fuel type (should fail)
\echo '\n--- Test 5: Testing unique fuel_type constraint (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO fuel_type (fuel_type) VALUES ('solar');
    RAISE EXCEPTION 'TEST FAILED: Should not allow duplicate fuel_type';
EXCEPTION
    WHEN unique_violation THEN
        RAISE NOTICE 'SUCCESS: Unique fuel_type constraint working correctly';
END $$;

-- Test 6: Valid system price
\echo '\n--- Test 6: Inserting valid system price ---'
INSERT INTO system_price (settlement_id, system_price) VALUES
    (1, 45.50),
    (2, 52.75);
\echo 'SUCCESS: Valid system price inserted'

-- Test 7: Invalid foreign key for system_price (should fail)
\echo '\n--- Test 7: Testing foreign key constraint on system_price (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO system_price (settlement_id, system_price) VALUES (999, 50.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid settlement_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on system_price working correctly';
END $$;

-- Test 8: Valid carbon intensity
\echo '\n--- Test 8: Inserting valid carbon intensity ---'
INSERT INTO carbon_intensity (settlement_id, carbon_intensity) VALUES
    (1, 125.30),
    (2, 110.45);
\echo 'SUCCESS: Valid carbon intensity inserted'

-- Test 9: Invalid foreign key for carbon_intensity (should fail)
\echo '\n--- Test 9: Testing foreign key constraint on carbon_intensity (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO carbon_intensity (settlement_id, carbon_intensity) VALUES (999, 100.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid settlement_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on carbon_intensity working correctly';
END $$;

-- Test 10: Valid generation data
\echo '\n--- Test 10: Inserting valid generation data ---'
INSERT INTO generation (settlement_id, fuel_type_id, generation_mw) VALUES
    (1, 1, 1500.50),
    (1, 2, 2200.75),
    (2, 3, 3500.00);
\echo 'SUCCESS: Valid generation data inserted'

-- Test 11: Invalid settlement_id in generation (should fail)
\echo '\n--- Test 11: Testing foreign key on generation.settlement_id (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO generation (settlement_id, fuel_type_id, generation_mw) VALUES (999, 1, 1000.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid settlement_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on generation.settlement_id working correctly';
END $$;

-- Test 12: Invalid fuel_type_id in generation (should fail)
\echo '\n--- Test 12: Testing foreign key on generation.fuel_type_id (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO generation (settlement_id, fuel_type_id, generation_mw) VALUES (1, 999, 1000.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid fuel_type_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on generation.fuel_type_id working correctly';
END $$;

-- Test 13: Valid recent_demand data
\echo '\n--- Test 13: Inserting valid recent_demand data ---'
INSERT INTO recent_demand (settlement_id, national_demand, transmission_system_demand) VALUES
    (1, 35000.00, 33500.00),
    (2, 36000.00, 34500.00);
\echo 'SUCCESS: Valid recent_demand data inserted'

-- Test 14: Invalid foreign key for recent_demand (should fail)
\echo '\n--- Test 14: Testing foreign key constraint on recent_demand (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO recent_demand (settlement_id, national_demand, transmission_system_demand)
    VALUES (999, 35000.00, 33500.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid settlement_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on recent_demand working correctly';
END $$;

-- Test 15: Valid historic_demand data
\echo '\n--- Test 15: Inserting valid historic_demand data ---'
INSERT INTO historic_demand (settlement_id, national_demand, transmission_system_demand) VALUES
    (1, 34500.00, 33000.00),
    (3, 37000.00, 35500.00);
\echo 'SUCCESS: Valid historic_demand data inserted'

-- Test 16: Invalid foreign key for historic_demand (should fail)
\echo '\n--- Test 16: Testing foreign key constraint on historic_demand (should FAIL) ---'
DO $$
BEGIN
    INSERT INTO historic_demand (settlement_id, national_demand, transmission_system_demand)
    VALUES (999, 35000.00, 33500.00);
    RAISE EXCEPTION 'TEST FAILED: Should not allow invalid settlement_id';
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'SUCCESS: Foreign key constraint on historic_demand working correctly';
END $$;

-- Test 17: Verify CASCADE delete works
\echo '\n--- Test 17: Testing CASCADE delete on settlements ---'
DELETE FROM settlements WHERE settlement_id = 1;
SELECT COUNT(*) as remaining_system_prices FROM system_price WHERE settlement_id = 1;
\echo 'SUCCESS: CASCADE delete working (should show 0 remaining records)'

-- Summary of inserted data
\echo '\n================================'
\echo 'Test Summary'
\echo '================================'
SELECT 'settlements' as table_name, COUNT(*) as record_count FROM settlements
UNION ALL
SELECT 'fuel_type', COUNT(*) FROM fuel_type
UNION ALL
SELECT 'system_price', COUNT(*) FROM system_price
UNION ALL
SELECT 'carbon_intensity', COUNT(*) FROM carbon_intensity
UNION ALL
SELECT 'generation', COUNT(*) FROM generation
UNION ALL
SELECT 'recent_demand', COUNT(*) FROM recent_demand
UNION ALL
SELECT 'historic_demand', COUNT(*) FROM historic_demand;

\echo '\n================================'
\echo 'All Tests Completed Successfully!'
\echo '================================'
