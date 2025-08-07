-- Create crm_erp database
CREATE DATABASE IF NOT EXISTS crm_erp;

-- Connect to crm_erp database
\c crm_erp;

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