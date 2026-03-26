-- raw table
CREATE TABLE raw_export_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

-- staging 
CREATE TABLE staging_export_data (
    unique_identifier VARCHAR(100),
    deal_number VARCHAR(100),
    decision VARCHAR(50),

    decision_date DATE,
    effective_date DATE,
    expiration_date DATE,
    fiscal_year INT,

    brokered VARCHAR(10),
    deal_cancelled VARCHAR(10),

    country VARCHAR(100),
    program VARCHAR(100),
    policy_type VARCHAR(100),
    decision_authority VARCHAR(100),

    naics_code VARCHAR(50),
    product_description TEXT,

    term VARCHAR(50),

    primary_applicant VARCHAR(255),
    primary_lender VARCHAR(255),
    primary_exporter VARCHAR(255),
    primary_exporter_city VARCHAR(100),
    primary_exporter_state_code VARCHAR(20),
    primary_exporter_state_name VARCHAR(100),

    primary_borrower VARCHAR(255),
    primary_source_of_repayment VARCHAR(255),

    working_capital_delegated_authority VARCHAR(50),

    approved_declined_amount DOUBLE,
    disbursed_shipped_amount DOUBLE,
    undisbursed_exposure_amount DOUBLE,
    outstanding_exposure_amount DOUBLE,

    small_business_authorized_amount DOUBLE,
    woman_owned_authorized_amount DOUBLE,
    minority_owned_authorized_amount DOUBLE,

    loan_interest_rate FLOAT,

    multiyear_working_capital_extension VARCHAR(50)
);

-- dim country
CREATE TABLE dim_country (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE
);

-- dim program
CREATE TABLE dim_program (
    program_id INT AUTO_INCREMENT PRIMARY KEY,
    program_name VARCHAR(100) UNIQUE,
    policy_type VARCHAR(100)
);

-- dim exporter 
CREATE TABLE dim_exporter (
    exporter_id INT AUTO_INCREMENT PRIMARY KEY,
    exporter_name VARCHAR(255),
    city VARCHAR(100),
    state_code VARCHAR(20),
    state_name VARCHAR(100)
);

-- dim day
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

-- fact approved deals
CREATE TABLE fact_approved_deals (
    deal_id VARCHAR(100),

    decision_date DATE,

    country_id INT,
    program_id INT,
    exporter_id INT,

    approved_amount DOUBLE,
    disbursed_amount DOUBLE,
    outstanding_amount DOUBLE,

    interest_rate FLOAT,

    PRIMARY KEY (deal_id),

    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id),
    FOREIGN KEY (exporter_id) REFERENCES dim_exporter(exporter_id)
);

-- fact declined deals
CREATE TABLE fact_declined_deals (
    deal_id VARCHAR(100),

    decision_date DATE,

    country_id INT,
    program_id INT,

    requested_amount DOUBLE,

    PRIMARY KEY (deal_id),

    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id)
);