from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os

def extract():
    """Extract and transform data from Bronze to Silver layer"""
    
    print("🚀 Starting Bronze to Silver ETL process...")
    
    try:
        # Check if HQL file exists
        hql_file = 'scripts/create-silver-schema.hql'
        print(f"📋 Checking for HQL file: {hql_file}")
        
        if not os.path.exists(hql_file):
            print(f"❌ HQL file not found: {hql_file}")
            print("📁 Current directory contents:")
            for item in os.listdir('.'):
                print(f"   - {item}")
            if os.path.exists('scripts'):
                print("📁 Scripts directory contents:")
                for item in os.listdir('scripts'):
                    print(f"   - scripts/{item}")
            sys.exit(1)
        
        print("✅ HQL file found, initializing Spark session...")
        
        # Initialize Spark Session with simpler configuration
        try:
            spark = SparkSession.builder \
                .appName("Bronze to Silver ETL") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.sql.hive.metastore.version", "4.0.1") \
                .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .enableHiveSupport() \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("✅ Spark session initialized successfully")
            
        except Exception as e:
            print(f"❌ Failed to initialize Spark session: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        # Test Hive connection
        print("🔗 Testing Hive connection...")
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            print(f"✅ Connected to Hive. Found {len(databases)} databases:")
            for db in databases:
                print(f"   - {db[0]}")
        except Exception as e:
            print(f"❌ Failed to connect to Hive: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
        # Check if bronze tables exist
        print("📊 Checking bronze tables...")
        try:
            bronze_tables = spark.sql("SHOW TABLES IN bronze").collect()
            print(f"✅ Found {len(bronze_tables)} bronze tables:")
            for table in bronze_tables:
                print(f"   - bronze.{table[1]}")
        except Exception as e:
            print(f"❌ Failed to access bronze database: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
        batch_start_time = datetime.now()
        print("=" * 48)
        print("Loading Silver Layer")
        print("=" * 48)
        
        # Create silver schema tables if not exists
        # create_silver_tables(spark)
        
        print("-" * 48)
        print("Loading CRM Tables")
        print("-" * 48)
        
        # Load CRM tables
        load_crm_cust_info(spark)
        load_crm_prd_info(spark)
        load_crm_sales_details(spark)
        
        print("-" * 48)
        print("Loading ERP Tables")
        print("-" * 48)
        
        # Load ERP tables
        load_erp_cust_az12(spark)
        load_erp_loc_a101(spark)
        load_erp_px_cat_g1v2(spark)
        
        batch_end_time = datetime.now()
        duration = (batch_end_time - batch_start_time).total_seconds()
        
        print("=" * 40)
        print("Loading Silver Layer is Completed")
        print(f"   - Total Load Duration: {duration:.0f} seconds")
        print("=" * 40)
        
    except Exception as e:
        print("=" * 40)
        print("ERROR OCCURRED DURING LOADING SILVER LAYER")
        print(f"Error Message: {str(e)}")
        print(f"Error Type: {type(e).__name__}")
        import traceback
        print("Full traceback:")
        traceback.print_exc()
        print("=" * 40)
        sys.exit(1)
    finally:
        try:
            if 'spark' in locals():
                print("🔄 Stopping Spark session...")
                spark.stop()
                print("✅ Spark session stopped")
        except Exception as e:
            print(f"⚠️ Error stopping Spark: {str(e)}")

def create_silver_tables(spark):
    """Create silver schema tables"""
    try:
        print("📋 Reading HQL file...")
        with open('scripts/create-silver-schema.hql', 'r') as file:
            hql_content = file.read()
        
        print("📋 Parsing HQL statements...")
        # Filter out comments and empty statements
        statements = []
        for stmt in hql_content.split(';'):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--') and not stmt.startswith('/*'):
                statements.append(stmt)
        
        print(f"📋 Found {len(statements)} statements to execute")
        
        for i, statement in enumerate(statements, 1):
            print(f"📋 Executing statement {i}/{len(statements)}...")
            # Print first 100 chars of statement for debugging
            stmt_preview = statement[:100].replace('\n', ' ').strip()
            print(f"    📝 {stmt_preview}...")
            try:
                spark.sql(statement)
                print(f"    ✅ Statement {i} executed successfully")
            except Exception as e:
                print(f"    ❌ Failed to execute statement {i}: {str(e)}")
                print(f"    📝 Full statement: {statement}")
                raise
        
        print("✅ Silver schema tables created/verified")
    except Exception as e:
        print(f"❌ Failed to create silver tables: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_crm_cust_info(spark):
    """Load and transform CRM customer info"""
    start_time = datetime.now()
    
    try:
        print(">> Checking if bronze.crm_cust_info exists...")
        bronze_count = spark.table("bronze.crm_cust_info").count()
        print(f">> Found {bronze_count:,} records in bronze.crm_cust_info")
        
        print(">> Inserting Data Into: silver.crm_cust_info")
        
        # Read from bronze and apply transformations
        bronze_df = spark.table("bronze.crm_cust_info")
        
        # Apply window function to get latest record per customer and transform
        transformed_df = bronze_df.filter(
            col("cst_id").isNotNull()
        ).withColumn(
            "flag_last",
            row_number().over(
                Window.partitionBy("cst_id").orderBy(desc("cst_create_date"))
            )
        ).filter(
            col("flag_last") == 1
        ).select(
            col("cst_id").cast("int"),
            col("cst_key"),
            trim(col("cst_firstname")).alias("cst_firstname"),
            trim(col("cst_lastname")).alias("cst_lastname"),
            when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .otherwise("n/a").alias("cst_marital_status"),
            when(upper(trim(col("cst_gndr"))) == "F", "Female")
            .when(upper(trim(col("cst_gndr"))) == "M", "Male")
            .otherwise("n/a").alias("cst_gndr"),
            col("cst_create_date").cast("date"),
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        print(">> Writing transformed data to silver.crm_cust_info...")
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.crm_cust_info")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load crm_cust_info: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_crm_prd_info(spark):
    """Load and transform CRM product info"""
    start_time = datetime.now()
    
    try:
        print(">> Inserting Data Into: silver.crm_prd_info")
        
        bronze_df = spark.table("bronze.crm_prd_info")
        
        # Transform product data - handle the case where prd_key might be the full key
        transformed_df = bronze_df.withColumn(
            "original_prd_key", col("prd_key")
        ).select(
            col("prd_id").cast("int"),
            # Extract category ID (first 5 chars, replace - with _)
            regexp_replace(substring(col("original_prd_key"), 1, 5), "-", "_").alias("cat_id"),
            # Extract product key (from 7th character onwards, or use original if too short)
            when(length(col("original_prd_key")) > 6, 
                 substring(col("original_prd_key"), 7, length(col("original_prd_key"))))
            .otherwise(col("original_prd_key")).alias("prd_key"),
            col("prd_nm"),
            coalesce(col("prd_cost"), lit(0)).cast("decimal(10,2)").alias("prd_cost"),
            # Map product line codes to descriptive values
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a").alias("prd_line"),
            col("prd_start_dt").cast("date"),
            # Calculate end date using LEAD function
            (lead(col("prd_start_dt")).over(
                Window.partitionBy(col("original_prd_key")).orderBy("prd_start_dt")
            ) - expr("INTERVAL 1 DAY")).cast("date").alias("prd_end_dt"),
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.crm_prd_info")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load crm_prd_info: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_crm_sales_details(spark):
    """Load and transform CRM sales details"""
    start_time = datetime.now()
    
    try:
        print(">> Inserting Data Into: silver.crm_sales_details")
        
        bronze_df = spark.table("bronze.crm_sales_details")
        
        # Transform sales data with date parsing and calculations
        transformed_df = bronze_df.select(
            col("sls_ord_num"),
            col("sls_prd_key"),
            col("sls_cust_id").cast("int"),
            # Parse date fields (handle string dates that may be 0 or wrong length)
            when((col("sls_order_dt").isNull()) | 
                 (col("sls_order_dt") == "0") | 
                 (length(col("sls_order_dt").cast("string")) != 8), None)
            .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")).alias("sls_order_dt"),
            
            when((col("sls_ship_dt").isNull()) | 
                 (col("sls_ship_dt") == "0") | 
                 (length(col("sls_ship_dt").cast("string")) != 8), None)
            .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")).alias("sls_ship_dt"),
            
            when((col("sls_due_dt").isNull()) | 
                 (col("sls_due_dt") == "0") | 
                 (length(col("sls_due_dt").cast("string")) != 8), None)
            .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")).alias("sls_due_dt"),
            
            # Recalculate sales if invalid
            when((col("sls_sales").isNull()) | 
                 (col("sls_sales") <= 0) | 
                 (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                 col("sls_quantity") * abs(col("sls_price")))
            .otherwise(col("sls_sales")).cast("int").alias("sls_sales"),
            
            col("sls_quantity").cast("int"),
            
            # Derive price if invalid
            when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                 col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity")).otherwise(1))
            .otherwise(col("sls_price")).cast("int").alias("sls_price"),
            
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.crm_sales_details")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load crm_sales_details: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_erp_cust_az12(spark):
    """Load and transform ERP customer data"""
    start_time = datetime.now()
    
    try:
        print(">> Inserting Data Into: silver.erp_cust_az12")
        
        bronze_df = spark.table("bronze.erp_cust_az12")
        
        # Transform ERP customer data
        transformed_df = bronze_df.select(
            # Remove 'NAS' prefix if present
            when(col("cid").startswith("NAS"), substring(col("cid"), 4, length(col("cid"))))
            .otherwise(col("cid")).alias("cid"),
            
            # Set future birthdates to NULL
            when(col("bdate") > current_date(), None)
            .otherwise(col("bdate")).cast("date").alias("bdate"),
            
            # Normalize gender values
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a").alias("gen"),
            
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.erp_cust_az12")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load erp_cust_az12: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_erp_loc_a101(spark):
    """Load and transform ERP location data"""
    start_time = datetime.now()
    
    try:
        print(">> Inserting Data Into: silver.erp_loc_a101")
        
        bronze_df = spark.table("bronze.erp_loc_a101")
        
        # Transform location data
        transformed_df = bronze_df.select(
            # Remove dashes from customer ID
            regexp_replace(col("cid"), "-", "").alias("cid"),
            
            # Normalize country codes
            when(trim(col("cntry")) == "DE", "Germany")
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry"))).alias("cntry"),
            
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.erp_loc_a101")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load erp_loc_a101: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_erp_px_cat_g1v2(spark):
    """Load and transform ERP product category data"""
    start_time = datetime.now()
    
    try:
        print(">> Inserting Data Into: silver.erp_px_cat_g1v2")
        
        bronze_df = spark.table("bronze.erp_px_cat_g1v2")
        
        # Transform category data (no transformation needed, direct copy)
        transformed_df = bronze_df.select(
            col("id"),
            col("cat"),
            col("subcat"),
            col("maintenance"),
            current_timestamp().alias("dwh_create_date")
        )
        
        # Write to silver
        transformed_df.write \
            .mode("overwrite") \
            .saveAsTable("silver.erp_px_cat_g1v2")
        
        record_count = transformed_df.count()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f">> Load Duration: {duration:.0f} seconds")
        print(f">> Records Processed: {record_count:,}")
        print(">> -------------")
        
    except Exception as e:
        print(f"❌ Failed to load erp_px_cat_g1v2: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

# Main execution
if __name__ == "__main__":
    print("=" * 50)
    print("Bronze to Silver ETL Process Starting")
    print("=" * 50)
    extract()