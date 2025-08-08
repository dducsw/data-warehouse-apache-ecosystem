from pyspark.sql import SparkSession
import sys

def view_gold_layer():
    print("Test using views in gold schema")

    spark = SparkSession.builder \
        .appName("View Gold Schema") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
        .config("spark.sql.hive.metastore.version", "4.0.1") \
        .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=== gold.dim_customers ===")
        spark.sql("SELECT * FROM gold.dim_customers LIMIT 10").show(truncate=False)
        spark.sql("SELECT COUNT(*) AS total FROM gold.dim_customers").show(truncate=False)

        print("=== gold.dim_products ===")
        spark.sql("SELECT * FROM gold.dim_products LIMIT 10").show(truncate=False)
        spark.sql("SELECT COUNT(*) AS total FROM gold.dim_products").show(truncate=False)

        print("=== gold.fact_sales ===")
        spark.sql("SELECT * FROM gold.fact_sales LIMIT 10").show(truncate=False)
        spark.sql("SELECT COUNT(*) AS total FROM gold.fact_sales").show(truncate=False)

    except Exception as e:
        print(f"ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    view_gold_layer()