-- =========================
-- init.sql
-- =========================
-- init.sql
CREATE DATABASE IF NOT EXISTS airflow_metadata;
GRANT ALL PRIVILEGES ON airflow_metadata.* TO 'airflow'@'%';
FLUSH PRIVILEGES;