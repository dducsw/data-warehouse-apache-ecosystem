/*
===============================================================================
DDL Script: Create Gold Schema Tables
===============================================================================
Script Purpose:
    This script creates tables for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each table contains materialized, clean, enriched, and business-ready dataset.

Usage:
    - These tables can be queried directly for analytics and reporting.
    - Data will be populated via ETL process from Silver layer
===============================================================================
*/

-- Create Gold Database
CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Gold Schema - Dimensional Model';

USE gold;

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    customer_key        BIGINT,         -- Surrogate key
    customer_id         INT,            -- Business key
    customer_number     STRING,         -- Customer number from source
    first_name          STRING,
    last_name           STRING,
    country             STRING,
    marital_status      STRING,
    gender              STRING,
    birthdate           DATE,
    create_date         DATE,
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='gold',
    'table_type'='dimension',
    'business_area'='customer'
);

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
    product_key         BIGINT,         -- Surrogate key
    product_id          INT,            -- Business key
    product_number      STRING,         -- Product number from source
    product_name        STRING,
    category_id         STRING,
    category_name       STRING,
    subcategory_name    STRING,
    maintenance_flag    STRING,
    cost                DECIMAL(10,2),
    product_line        STRING,
    start_date          DATE,
    end_date            DATE,
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='gold',
    'table_type'='dimension',
    'business_area'='product'
);

-- =============================================================================
-- Create Dimension: gold.dim_product_categories
-- =============================================================================
DROP TABLE IF EXISTS dim_product_categories;

CREATE TABLE dim_product_categories (
    category_key        BIGINT,         -- Surrogate key
    category_id         STRING,         -- Business key
    category_name       STRING,
    subcategory_name    STRING,
    maintenance_flag    STRING,
    dwh_create_date     TIMESTAMP,
    dwh_update_date     TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='gold',
    'table_type'='dimension',
    'business_area'='product'
);



-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
    -- Keys
    sales_key           BIGINT,         -- Surrogate key
    order_number        STRING,         -- Business key
    product_key         BIGINT,         -- FK to dim_products
    customer_key        BIGINT,         -- FK to dim_customers
    
    -- Dates
    order_date          DATE,
    shipping_date       DATE,
    due_date            DATE,
    
    -- Measures
    sales_amount        INT,
    quantity            INT,
    unit_price          INT,
    total_sales         DECIMAL(15,2),  -- Calculated field
    cost_amount         DECIMAL(15,2),  -- Calculated field
    profit_amount       DECIMAL(15,2),  -- Calculated field
    profit_margin_pct   DECIMAL(5,2),   -- Calculated field
    
    
    -- Audit
    dwh_create_date     TIMESTAMP,
    dwh_update_date     TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'layer'='gold',
    'table_type'='fact',
    'business_area'='sales'
);

