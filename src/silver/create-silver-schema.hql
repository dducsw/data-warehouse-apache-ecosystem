/*
===============================================================================
DDL Script: Create Silver Schema Tables in Hive
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of silver Tables
===============================================================================
*/

-- Create Silver Database
CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Silver Schema';

USE silver;

-- Drop and recreate CRM customer info table
DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            STRING,
    cst_firstname      STRING,
    cst_lastname       STRING,
    cst_marital_status STRING,
    cst_gndr           STRING,
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='crm',
    'table_type'='customer'
);

-- Drop and recreate CRM product info table
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          STRING,
    prd_key         STRING,
    prd_nm          STRING,
    prd_cost        DECIMAL(10,2),
    prd_line        STRING,
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='crm',
    'table_type'='product'
);

-- Drop and recreate CRM sales details table
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     STRING,
    sls_prd_key     STRING,
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='crm',
    'table_type'='sales'
);

-- Drop and recreate ERP location table
DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid             STRING,
    cntry           STRING,
    dwh_create_date TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='erp',
    'table_type'='location'
);

-- Drop and recreate ERP customer table
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid             STRING,
    bdate           DATE,
    gen             STRING,
    dwh_create_date TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='erp',
    'table_type'='customer'
);

-- Drop and recreate ERP product category table
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              STRING,
    cat             STRING,
    subcat          STRING,
    maintenance     STRING,
    dwh_create_date TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='silver',
    'source_system'='erp',
    'table_type'='product_category'
);
