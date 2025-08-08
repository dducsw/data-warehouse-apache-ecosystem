from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_cust_info():
    print("Transform erp_cust_az12 to silver layer")

    spark = SparkSession.builder \
            .appName("Transform erp_cust_az12 to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Start transform erp_cust_az12")
        
        batch_start_time = datetime.now()

        # Read data from the bronze layer table, filtering for records updated in the last day
        df = spark.table("bronze.erp_cust_az12") \
            .filter(col("src_update_at") > (current_timestamp() - expr("INTERVAL 1 DAY")))

        # Transform columns:
        # - 'cid': Remove 'NAS' prefix if present
        # - 'bdate': Set to None if birthdate is in the future, otherwise cast to date
        # - 'gen': Normalize gender values to 'Female', 'Male', or 'n/a'
        # - 'dwh_create_date': Add current timestamp for data warehouse creation date
        out = df.select(
            when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100)).otherwise(col("cid")).alias("cid"),
            when(col("bdate") > current_date(), None).otherwise(col("bdate")).cast("date").alias("bdate"),
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a").alias("gen"),
            current_timestamp().alias("dwh_create_date")
        )

        # Write the transformed data to the silver layer table, overwriting existing data
        out.write.mode("overwrite").saveAsTable("silver.erp_cust_az12")

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