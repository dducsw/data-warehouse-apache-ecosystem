from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_cust_info():
    print("Transform erp_px_cat_g1v2 to silver layer")

    spark = SparkSession.builder \
            .appName("Transform erp_px_cat_g1v2 to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Start transform erp_px_cat_g1v2")
        
        batch_start_time = datetime.now()

        df = spark.table("bronze.erp_px_cat_g1v2")
        out = df.select(
            col("id"), col("cat"), col("subcat"), col("maintenance"),
            current_timestamp().alias("dwh_create_date")
        )
        out.write.mode("overwrite").saveAsTable("silver.erp_px_cat_g1v2")

        duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"== Silver Layer Loaded in {duration:.0f} seconds")
    except Exception as e:
        print(f" ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_crm_cust_info()