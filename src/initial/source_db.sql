-- Create crm_erp database
CREATE DATABASE crm_erp;

-- Connect to crm_erp database
\connect crm_erp;

-- Drop and recreate CRM customer info table
DROP TABLE IF EXISTS crm_cust_info;

CREATE TABLE crm_cust_info (
    cst_id             INTEGER,
    cst_key            VARCHAR(100),
    cst_firstname      VARCHAR(100),
    cst_lastname       VARCHAR(100),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(10),
    cst_create_date    DATE
);

-- Drop and recreate CRM product info table
DROP TABLE IF EXISTS crm_prd_info;

CREATE TABLE crm_prd_info (
    prd_id          INTEGER,
    prd_key         VARCHAR(100),
    prd_nm          VARCHAR(200),
    prd_cost        VARCHAR(100),    
    prd_line        VARCHAR(100),
    prd_start_dt    TIMESTAMP,
    prd_end_dt      TIMESTAMP
);

-- Drop and recreate CRM sales details table
DROP TABLE IF EXISTS crm_sales_details;

CREATE TABLE crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INTEGER,
    sls_order_dt    INTEGER,     
    sls_ship_dt     INTEGER,    
    sls_due_dt      INTEGER,     
    sls_sales       INTEGER,
    sls_quantity    INTEGER,
    sls_price       INTEGER
);

-- Drop and recreate ERP location table
DROP TABLE IF EXISTS erp_loc_a101;

CREATE TABLE erp_loc_a101 (
    cid             VARCHAR(100),
    cntry           VARCHAR(100)
);

-- Drop and recreate ERP customer table
DROP TABLE IF EXISTS erp_cust_az12;

CREATE TABLE erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50)
);

-- Drop and recreate ERP product category table
DROP TABLE IF EXISTS erp_px_cat_g1v2;

CREATE TABLE erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50)
);

-- Create ETL Info Database
CREATE DATABASE etl_info;

-- Connect to etl_info database
\c etl_info;

-- ETL Job Configuration Table
DROP TABLE IF EXISTS job_config;

CREATE TABLE job_config (
    config_id           VARCHAR(100),
    job_name            VARCHAR(100),
    source_schema       VARCHAR(100),
    source_table        VARCHAR(100),
    source_db_type      VARCHAR(100),
    source_ip           VARCHAR(100),
    destination_schema  VARCHAR(100),
    destination_table   VARCHAR(100),
    destination_db_type VARCHAR(100),
    destination_ip      VARCHAR(100),
    load_type           INTEGER,        -- 0: full, 1: incremental
    schedule_type       VARCHAR(100),
    is_active           INTEGER,
    created_date        TIMESTAMP
);

-- ETL Job Log Table
DROP TABLE IF EXISTS job_log;

CREATE TABLE job_log (
    log_id              VARCHAR(100),
    job_name            VARCHAR(100),
    run_date            DATE,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    record_count        BIGINT,
    status              VARCHAR(50),     -- RUNNING, SUCCESS, FAILED
    error_message       VARCHAR(255),
    created_date        TIMESTAMP
);
