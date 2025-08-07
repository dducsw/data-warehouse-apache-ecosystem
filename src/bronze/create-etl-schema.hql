CREATE DATABASE IF NOT EXISTS job_info;
COMMENT 'ETL Job schema';

USE job_info;

-- ETL Job Configuration Table
CREATE TABLE IF NOT EXISTS job_config (
    config_id           STRING,
    job_name            STRING,
    source_schema       STRING,
    source_table        STRING,
    source_db_type      STRING,
    source_ip           STRING,
    destination_schema  STRING,
    destination_table   STRING,
    destination_db_type STRING,
    destination_ip      STRING,
    load_type           INT,        -- 0: full, 1: incremental
    schedule_type       STRING,
    is_active           INT,
    created_date        TIMESTAMP
)
STORED AS PARQUET;

-- ETL Job Log Table
CREATE TABLE IF NOT EXISTS job_log (
    log_id              STRING,
    job_name            STRING,
    run_date            DATE,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    record_count        BIGINT,
    status              STRING,
    error_message       STRING,
    created_date        TIMESTAMP
)
STORED AS PARQUET;
