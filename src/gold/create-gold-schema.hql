-- Create Gold Database
CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Gold Schema - Dimensional Model';

USE gold;

-- =============================================================================
-- Create Table: gold.dim_customers
-- =============================================================================
DROP TABLE IF EXISTS gold.dim_customers;

CREATE TABLE gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

-- =============================================================================
-- Create Table: gold.dim_products
-- =============================================================================
DROP TABLE IF EXISTS gold.dim_products;

CREATE TABLE gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category_name,
    pc.subcat       AS subcategory_name,
    pc.maintenance  AS maintenance_flag,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date,
    pn.prd_end_dt   AS end_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id;

-- =============================================================================
-- Create Table: gold.fact_sales
-- =============================================================================
DROP TABLE IF EXISTS gold.fact_sales;

CREATE TABLE gold.fact_sales AS
SELECT
    sd.sls_ord_num      AS order_number,
    pr.product_key      AS product_key,
    cu.customer_key     AS customer_key,
    sd.sls_order_dt     AS order_date,
    sd.sls_ship_dt      AS shipping_date,
    sd.sls_due_dt       AS due_date,
    sd.sls_sales        AS sales_amount,
    sd.sls_quantity     AS quantity,
    sd.sls_price        AS unit_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
