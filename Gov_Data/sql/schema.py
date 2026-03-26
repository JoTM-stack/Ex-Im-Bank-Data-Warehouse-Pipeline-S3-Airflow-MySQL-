import pymysql
from src.utils.logger import get_logger

logger = get_logger()

DB_CONFIG = {
    "host": "mysql",
    "user": "root",
    "password": "root123",
    "port": 3306
}

SQL = """
    
-- =========================
-- RESET DATABASE (OPTIONAL)
-- =========================
DROP DATABASE IF EXISTS govdata;
CREATE DATABASE govdata;
USE govdata;

-- =========================
-- RAW TABLE
-- =========================
CREATE TABLE raw_export_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

-- =========================
-- STAGING TABLE (UPDATED)
-- =========================
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
    country_clean VARCHAR(100),  

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

-- =========================
-- DIMENSIONS
-- =========================

-- COUNTRY DIM (use abbreviation as ID )
CREATE TABLE dim_country (
    country_id VARCHAR(10) PRIMARY KEY,  
    country_name VARCHAR(100) UNIQUE
);

-- PROGRAM DIM
CREATE TABLE dim_program (
    program_id INT AUTO_INCREMENT PRIMARY KEY,
    program_name VARCHAR(100),
    policy_type VARCHAR(100),
    UNIQUE(program_name, policy_type)
);

-- EXPORTER DIM
CREATE TABLE dim_exporter (
    exporter_id INT AUTO_INCREMENT PRIMARY KEY,
    exporter_name VARCHAR(255),
    city VARCHAR(100),
    state_code VARCHAR(20),
    state_name VARCHAR(100),
    UNIQUE(exporter_name, city, state_code)
);

-- DATE DIM
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE fact_approved_deals (
    deal_id VARCHAR(100) PRIMARY KEY,

    date_id DATE,

    country_id VARCHAR(10),
    program_id INT,
    exporter_id INT,

    approved_amount DOUBLE,
    disbursed_amount DOUBLE,
    outstanding_amount DOUBLE,
    interest_rate FLOAT,

    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id),
    FOREIGN KEY (exporter_id) REFERENCES dim_exporter(exporter_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_declined_deals (
    deal_id VARCHAR(100) PRIMARY KEY,

    date_id DATE,

    country_id VARCHAR(10),
    program_id INT,

    requested_amount DOUBLE,

    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
"""

def run_schema():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    statements = [s.strip() for s in SQL.split(";") if s.strip()]

    for statement in statements:
        try:
            cursor.execute(statement)
            logger.info(f"Executed: {statement[:60]}...")
        except Exception as e:
            logger.error(f"Failed: {e}")
            raise

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Schema setup complete")

if __name__ == "__main__":
    run_schema()