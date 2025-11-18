CREATE TABLE settlements(
    settlement_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_date DATE NOT NULL,
    -- settlement period 1 to 48 for half-hourly periods
    settlement_period INT NOT NULL,
    CONSTRAINT settlement_period_check CHECK (settlement_period BETWEEN 1 AND 48)
);

CREATE TABLE system_prices(
    price_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_id INT NOT NULL,
    system_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id)
);

CREATE TABLE carbon_intensity(
    intensity_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_id INT NOT NULL,
    carbon_intensity DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id)
);

CREATE TABLE generation(
    generation_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_id INT NOT NULL,
    fuel_type_id INT NOT NULL,
    generation_mw DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id)
    FOREIGN KEY (fuel_type_id) REFERENCES fuel_type(fuel_type_id)
);

CREATE TABLE fuel_type(
    fuel_type_id INT IDENTITY(1,1) PRIMARY KEY,
    fuel_type VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255)
);

CREATE TABLE recent_demand(
    demand_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_id INT NOT NULL,
    national_demand DECIMAL(10,2) NOT NULL,
    transmission_system_demand DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id)
);

CREATE TABLE historic_demand(
    demand_id INT IDENTITY(1,1) PRIMARY KEY,
    settlement_id INT NOT NULL,
    national_demand DECIMAL(10,2) NOT NULL,
    transmission_system_demand DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id)
);