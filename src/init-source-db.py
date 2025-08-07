from pyspark.sql import SparkSession
import sys

def create_tables():
    try:
        spark = SparkSession.builder \
            .appName("Create Hive Tables") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        # Read the HQL file
        with open('scripts/create-source-db.hql', 'r') as file:
            hql_content = file.read()
        
        # Split and execute statements
        statements = [stmt.strip() for stmt in hql_content.split(';') if stmt.strip()]
        
        success_count = 0
        error_count = 0
        
        for i, statement in enumerate(statements):
            if statement.strip() and not statement.strip().startswith('--'):
                print(f"\n[{i+1}/{len(statements)}] Executing: {statement[:100]}...")
                try:
                    spark.sql(statement)
                    print("✅ Success")
                    success_count += 1
                except Exception as e:
                    print(f"❌ Error: {str(e)}...")
                    error_count += 1
                    # Continue với các statement khác thay vì break
                    continue
        
        # Load CSV data into tables
        print("\n🔄 Loading CSV data into tables...")
        load_csv_data(spark)
        
        

        print(f"\n📊 Summary:")
        print(f"✅ Successful statements: {success_count}")
        print(f"❌ Failed statements: {error_count}")
        print(f"📝 Total statements: {success_count + error_count}")
        
        if error_count == 0:
            print("🎉 All tables created successfully!")
        else:
            print("⚠️  Some statements failed, but process completed.")
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

def load_csv_data(spark):
    """Load CSV data into Hive tables"""
    import os
    from datetime import datetime
    from pyspark.sql.functions import current_timestamp, lit
    
    # Base path for CSV files
    base_path = "file:///home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data"
    local_base_path = "/home/ldduc/D/data-warehouse-apache-ecosystem/data/raw_data"
    
    # Define table mappings: (csv_file_path, table_name, source_file_name)
    table_mappings = [
        ("source_crm/cust_info.csv", "bronze.crm_cust_info", "cust_info.csv"),
        ("source_crm/prd_info.csv", "bronze.crm_prd_info", "prd_info.csv"),
        ("source_crm/sales_details.csv", "bronze.crm_sales_details", "sales_details.csv"),
        ("source_erp/LOC_A101.csv", "bronze.erp_loc_a101", "LOC_A101.csv"),
        ("source_erp/CUST_AZ12.csv", "bronze.erp_cust_az12", "CUST_AZ12.csv"),
        ("source_erp/PX_CAT_G1V2.csv", "bronze.erp_px_cat_g1v2", "PX_CAT_G1V2.csv")
    ]
    
    for csv_path, table_name, source_file in table_mappings:
        # For Spark load
        spark_path = f"{base_path}/{csv_path}"
        # For local file check
        local_path = os.path.join(local_base_path, csv_path)
        
        if os.path.exists(local_path):
            print(f"\n📁 Loading {csv_path} into {table_name}...")
            try:
                # Read CSV file
                df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "false") \
                    .load(spark_path)
                
                # Add metadata columns
                df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumn("source_file", lit(source_file))
                
                # Write to Hive table (overwrite mode)
                df_with_metadata.write \
                    .mode("overwrite") \
                    .format("hive") \
                    .saveAsTable(table_name)
                
                row_count = df.count()
                print(f"✅ Loaded {row_count} rows into {table_name}")
                
            except Exception as e:
                print(f"❌ Failed to load {csv_path}: {str(e)}")
        else:
            print(f"⚠️  File not found: {local_path}")
            print(f"📂 Please check if the file exists at: {local_path}")

if __name__ == "__main__":
    create_tables()



