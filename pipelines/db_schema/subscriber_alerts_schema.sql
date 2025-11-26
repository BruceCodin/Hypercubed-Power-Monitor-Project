DROP TABLE IF EXISTS BRIDGE_subscribed_postcodes;
DROP TABLE IF EXISTS BRIDGE_affected_postcodes;
DROP TABLE IF EXISTS FACT_outage;
DROP TABLE IF EXISTS DIM_customer;
DROP TABLE IF EXISTS FACT_notification_log;


CREATE TABLE DIM_customer (
    customer_id INT GENERATED ALWAYS AS IDENTITY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    PRIMARY KEY (customer_id),
    CONSTRAINT email_format CHECK (email LIKE '%_@__%.__%')
);

CREATE TABLE FACT_outage (
    outage_id INT GENERATED ALWAYS AS IDENTITY,
    source_provider TEXT NOT NULL,
    status TEXT,
    outage_date DATE,
    recording_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (outage_id)
);

CREATE TABLE BRIDGE_affected_postcodes (
    affected_id INT GENERATED ALWAYS AS IDENTITY,
    outage_id INT NOT NULL,
    postcode_affected TEXT NOT NULL,
    PRIMARY KEY (affected_id),
    FOREIGN KEY (outage_id) REFERENCES FACT_outage(outage_id) ON DELETE CASCADE
);

CREATE TABLE BRIDGE_subscribed_postcodes (
    subscription_id INT GENERATED ALWAYS AS IDENTITY,
    customer_id INT NOT NULL,
    postcode TEXT NOT NULL,
    UNIQUE (customer_id, postcode),
    PRIMARY KEY (subscription_id),
    FOREIGN KEY (customer_id) REFERENCES DIM_customer(customer_id) ON DELETE CASCADE
);

CREATE TABLE FACT_notification_log (
    notification_id SERIAL PRIMARY KEY,
    customer_id INT,
    outage_id INT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Ensure we never email the same person about the same outage twice
    UNIQUE(customer_id, outage_id)
    FOREIGN KEY (customer_id) REFERENCES DIM_customer(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (outage_id) REFERENCES FACT_outage(outage_id) ON DELETE CASCADE
);