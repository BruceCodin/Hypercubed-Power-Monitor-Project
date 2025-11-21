DROP TABLE IF EXISTS historic_demand CASCADE;
DROP TABLE IF EXISTS recent_demand CASCADE;
DROP TABLE IF EXISTS generation CASCADE;
DROP TABLE IF EXISTS fuel_type CASCADE;
DROP TABLE IF EXISTS carbon_intensity CASCADE;
DROP TABLE IF EXISTS system_price CASCADE;
DROP TABLE IF EXISTS settlements CASCADE;

CREATE TABLE settlements(
    settlement_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_date TIMESTAMP NOT NULL,
    settlement_period INT NOT NULL,
    CONSTRAINT settlement_period_check CHECK (settlement_period BETWEEN 1 AND 48)
);

CREATE TABLE system_price(
    price_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_id INT NOT NULL,
    system_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id) ON DELETE CASCADE
);

CREATE TABLE carbon_intensity(
    intensity_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_id INT NOT NULL,
    carbon_intensity DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id) ON DELETE CASCADE
);

CREATE TABLE fuel_type(
    fuel_type_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fuel_type TEXT UNIQUE NOT NULL
);

CREATE TABLE generation(
    generation_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_id INT NOT NULL,
    fuel_type_id INT NOT NULL,
    generation_mw DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id) ON DELETE CASCADE,
    FOREIGN KEY (fuel_type_id) REFERENCES fuel_type(fuel_type_id) ON DELETE CASCADE
    );

CREATE TABLE recent_demand(
    demand_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_id INT NOT NULL,
    national_demand DECIMAL(10,2) NOT NULL,
    transmission_system_demand DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id) ON DELETE CASCADE,
    CONSTRAINT recent_demand_unique UNIQUE (settlement_id)

);

CREATE TABLE historic_demand(
    demand_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement_id INT NOT NULL,
    national_demand DECIMAL(10,2) NOT NULL,
    transmission_system_demand DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (settlement_id) REFERENCES settlements(settlement_id) ON DELETE CASCADE,
    CONSTRAINT historic_demand_unique UNIQUE (settlement_id)
);