DROP TABLE IF EXISTS FACT_notification_log;

CREATE TABLE FACT_notification_log (
    notification_id SERIAL PRIMARY KEY,
    customer_id INT,
    outage_id INT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Ensure we never email the same person about the same outage twice
    UNIQUE(customer_id, outage_id) 
);