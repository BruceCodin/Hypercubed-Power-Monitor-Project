DROP TABLE IF EXISTS BRIDGE_subscribed_postcodes;
DROP TABLE IF EXISTS BRIDGE_affected_postcodes;
DROP TABLE IF EXISTS FACT_outage;
DROP TABLE IF EXISTS DIM_customer;

CREATE TABLE IF NOT EXISTS DIM_customer (
    customer_id INT UNIQUE GENERATED ALWAYS AS IDENTITY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS FACT_outage (
    outage_id INT UNIQUE GENERATED ALWAYS AS IDENTITY,
    source_provider TEXT NOT NULL,
    status TEXT,
    region_affected TEXT,
    outage_date DATE,
    recording_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (outage_id)
);

CREATE TABLE IF NOT EXISTS BRIDGE_affected_postcodes (
    affected_id INT UNIQUE GENERATED ALWAYS AS IDENTITY,
    outage_id INT NOT NULL,
    postcode_affected TEXT NOT NULL,
    PRIMARY KEY (affected_id),
    FOREIGN KEY (outage_id) REFERENCES FACT_outage(outage_id)
);

CREATE TABLE IF NOT EXISTS BRIDGE_subscribed_postcodes (
    subscription_id INT UNIQUE GENERATED ALWAYS AS IDENTITY,
    customer_id INT NOT NULL,
    postcode TEXT NOT NULL,
    PRIMARY KEY (subscription_id),
    FOREIGN KEY (customer_id) REFERENCES DIM_customer(customer_id)
);