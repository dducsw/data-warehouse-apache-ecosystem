-- Create Bronze Database
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Bronze layer - Raw data from source systems';

USE bronze;

-- CRM customer info table
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
    cst_id STRING,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm'
);

-- CRM product info table
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id STRING,
    prd_key STRING,
    prd_nm STRING,
    prd_cost STRING,
    prd_line STRING,
    prd_start_dt STRING,
    prd_end_dt STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm'
);

-- CRM sales details table
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id STRING,
    sls_order_dt STRING,
    sls_ship_dt STRING,
    sls_due_dt STRING,
    sls_sales STRING,
    sls_quantity STRING,
    sls_price STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm'
);

-- ERP location table
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid STRING,
    cntry STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp'
);

-- ERP customer table
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid STRING,
    bdate STRING,
    gen STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp'
);

-- ERP product category table
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id STRING,
    cat STRING,
    subcat STRING,
    maintenance STRING
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp'
);

