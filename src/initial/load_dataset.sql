/*
===============================================================================
Function: Load Dataset (Source -> Tables)
===============================================================================
Script Purpose:
    This function loads data into the tables from external CSV files. 
    It performs the following actions:
    - Truncates the tables before loading data.
    - Uses the COPY command to load data from CSV files to tables.
    - Counts and reports the number of records loaded in each table.

Parameters:
    None. 
    This function does not accept any parameters.

Usage Example:
    SELECT load_dataset();
===============================================================================
*/

-- Connect to crm_erp database
\c crm_erp;

-- Create the load_dataset function
CREATE OR REPLACE FUNCTION load_dataset()
RETURNS TEXT AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
    total_duration_seconds INTEGER;
    record_count INTEGER;
    total_records INTEGER := 0;
BEGIN
    batch_start_time := clock_timestamp();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Dataset';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Load CRM Customer Info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE crm_cust_info;
    
    RAISE NOTICE '>> Inserting Data Into: crm_cust_info';
    COPY crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_crm/cust_info.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM crm_cust_info;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load CRM Product Info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE crm_prd_info;
    
    RAISE NOTICE '>> Inserting Data Into: crm_prd_info';
    
    -- Use temp table approach for flexible column handling (7 columns only)
    DROP TABLE IF EXISTS temp_prd_info;
    CREATE TEMP TABLE temp_prd_info (
        col1 TEXT,
        col2 TEXT,
        col3 TEXT,
        col4 TEXT,
        col5 TEXT,
        col6 TEXT,
        col7 TEXT
    );

    COPY temp_prd_info FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_crm/prd_info.csv' 
    WITH (
        FORMAT CSV, 
        HEADER true, 
        QUOTE '"',
        DELIMITER ',',
        NULL ''
    );

    INSERT INTO crm_prd_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        CASE WHEN col1 = '' OR col1 IS NULL THEN NULL 
             ELSE col1::INTEGER END,
        NULLIF(col2, ''),
        NULLIF(col3, ''),
        NULLIF(col4, ''),  -- Keep as VARCHAR as defined in source_db.sql
        NULLIF(col5, ''),
        CASE WHEN col6 = '' OR col6 IS NULL THEN NULL 
             ELSE col6::TIMESTAMP END,
        CASE WHEN col7 = '' OR col7 IS NULL THEN NULL 
             ELSE col7::TIMESTAMP END
    FROM temp_prd_info
    WHERE col1 IS NOT NULL;
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM crm_prd_info;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load CRM Sales Details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: crm_sales_details';
    TRUNCATE TABLE crm_sales_details;
    
    RAISE NOTICE '>> Inserting Data Into: crm_sales_details';
    COPY crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_crm/sales_details.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM crm_sales_details;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Load ERP Location Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: erp_loc_a101';
    TRUNCATE TABLE erp_loc_a101;
    
    RAISE NOTICE '>> Inserting Data Into: erp_loc_a101';
    COPY erp_loc_a101 (
        cid,
        cntry
    )
    FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_erp/LOC_A101.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM erp_loc_a101;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load ERP Customer Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: erp_cust_az12';
    TRUNCATE TABLE erp_cust_az12;
    
    RAISE NOTICE '>> Inserting Data Into: erp_cust_az12';
    COPY erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM erp_cust_az12;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load ERP Product Category Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: erp_px_cat_g1v2';
    TRUNCATE TABLE erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Inserting Data Into: erp_px_cat_g1v2';
    COPY erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    FROM '/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM erp_px_cat_g1v2;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    batch_end_time := clock_timestamp();
    total_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Dataset is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', total_duration_seconds;
    RAISE NOTICE '   - Total Records Loaded: %', total_records;
    RAISE NOTICE '==========================================';

    -- Summary of all table record counts
    RAISE NOTICE '';
    RAISE NOTICE 'Table Record Count Summary:';
    RAISE NOTICE '   - crm_cust_info: % records', (SELECT COUNT(*) FROM crm_cust_info);
    RAISE NOTICE '   - crm_prd_info: % records', (SELECT COUNT(*) FROM crm_prd_info);
    RAISE NOTICE '   - crm_sales_details: % records', (SELECT COUNT(*) FROM crm_sales_details);
    RAISE NOTICE '   - erp_loc_a101: % records', (SELECT COUNT(*) FROM erp_loc_a101);
    RAISE NOTICE '   - erp_cust_az12: % records', (SELECT COUNT(*) FROM erp_cust_az12);
    RAISE NOTICE '   - erp_px_cat_g1v2: % records', (SELECT COUNT(*) FROM erp_px_cat_g1v2);

    RETURN 'Dataset loading completed successfully in ' || total_duration_seconds || ' seconds with ' || total_records || ' total records loaded';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING DATASET';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error State: %', SQLSTATE;
        RAISE NOTICE '==========================================';
        RETURN 'ERROR: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Execute the function
SELECT load_dataset();