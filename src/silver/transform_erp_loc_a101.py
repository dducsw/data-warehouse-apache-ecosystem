from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_cust_info():
    print("Transform erp_loc_a101 to silver layer")

    spark = SparkSession.builder \
            .appName("Transform erp_loc_a101 to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Start transform erp_loc_a101")
        
        batch_start_time = datetime.now()

        # Read source table from bronze layer and filter for records updated in the last day
        df = spark.table("bronze.erp_loc_a101")

        # Standardize country field and customer ID, add ETL load timestamp
        out = df.select(
            regexp_replace(col("cid"), "-", "").alias("cid"),  # Remove dashes from customer ID
            when(trim(col("cntry")) == "DE", "Germany")        # Standardize country: DE->Germany, US/USA->United States, blank/null->n/a
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry"))).alias("cntry"),
            current_timestamp().alias("dwh_create_date")       # Add ETL load timestamp
        ).dropDuplicates(["cid"])

        # Write transformed data to silver layer
        out.write.mode("overwrite").saveAsTable("silver.erp_loc_a101")

        number_record = out.count()
        duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"== Silver Layer Loaded {number_record} records in {duration:.0f} seconds")
    except Exception as e:
        print(f" ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_crm_cust_info()