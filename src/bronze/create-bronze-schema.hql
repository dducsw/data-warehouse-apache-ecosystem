-- Create Bronze Database
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Bronze layer - Raw data from source systems';

-- CRM customer info table
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm',
    'source_table'='crm_cust_info',
    'parquet.compression'='snappy'
);

-- CRM product info table
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key STRING,
    prd_nm STRING,
    prd_cost STRING,
    prd_line STRING,
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm',
    'source_table'='crm_prd_info',
    'parquet.compression'='snappy'
);

-- CRM sales details table
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='crm',
    'source_table'='crm_sales_details',
    'parquet.compression'='snappy'
);

-- ERP location table
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid STRING,
    cntry STRING,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp',
    'source_table'='erp_loc_a101',
    'parquet.compression'='snappy'
);

-- ERP customer table
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid STRING,
    bdate DATE,
    gen STRING,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp',
    'source_table'='erp_cust_az12',
    'parquet.compression'='snappy'
);

-- ERP product category table
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id STRING,
    cat STRING,
    subcat STRING,
    maintenance STRING,
    src_update_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='bronze',
    'source_system'='erp',
    'source_table'='erp_px_cat_g1v2',
    'parquet.compression'='snappy'
);


