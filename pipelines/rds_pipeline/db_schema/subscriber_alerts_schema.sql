CREATE TABLE IF NOT EXISTS DIM_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) UNIQUE NOT NULL,
    signup_date DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);