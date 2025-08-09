from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_cust_info():
    print("Transform crm_cust_info to silver layer")

    spark = SparkSession.builder \
            .appName("Transform crm_cust_info to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Start transform crm_cust_info")
        
        batch_start_time = datetime.now()

        # Read source table from bronze layer and filter for records updated in the last day
        df = spark.table("bronze.crm_cust_info")

        # Define window to select the latest record per customer based on create date
        w = Window.partitionBy("cst_id").orderBy(desc("cst_create_date"))

        # Filter out records with null customer ID, select latest record per customer,
        # and standardize key fields (marital status, gender, names)
        out = df.filter(col("cst_id").isNotNull()) \
            .withColumn("flag_last", row_number().over(w)) \
            .filter(col("flag_last") == 1) \
            .select(
            col("cst_id").cast("int"),  # Cast customer ID to integer
            col("cst_key"),
            trim(col("cst_firstname")).alias("cst_firstname"),  # Remove whitespace from first name
            trim(col("cst_lastname")).alias("cst_lastname"),    # Remove whitespace from last name
            when(upper(trim(col("cst_marital_status"))) == "S", "Single") # Standardize marital status: S->Single, M->Married, else n/a
                .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
                .otherwise("n/a").alias("cst_marital_status"),
            when(upper(trim(col("cst_gndr"))) == "F", "Female") # Standardize gender: F->Female, M->Male, else n/a
                .when(upper(trim(col("cst_gndr"))) == "M", "Male")
                .otherwise("n/a").alias("cst_gndr"),
            col("cst_create_date").cast("date"),  # Cast create date to date type
            current_timestamp().alias("dwh_create_date")  # Add ETL load timestamp
            ).dropDuplicates(["cst_id"])

        out.write.mode("overwrite").saveAsTable("silver.crm_cust_info")
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