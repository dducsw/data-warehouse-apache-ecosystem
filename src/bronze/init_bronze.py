from pyspark.sql import SparkSession

def create_tables():
    spark = SparkSession.builder \
        .appName("Create Bronze Layer in DW") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
        .config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
        .config("spark.sql.hive.metastore.version", "4.0.1") \
        .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        with open('create-bronze-schema.hql', 'r') as file:
            statements = [stmt.strip() for stmt in file.read().split(';') if stmt.strip() and not stmt.strip().startswith('--')]
        success_count = error_count = 0
        for i, statement in enumerate(statements, 1):
            print(f"\n[{i}/{len(statements)}] Executing: {statement[:200]}...")
            try:
                spark.sql(statement)
                print("== Success")
                success_count += 1
            except Exception as e:
                print(f"-- Error: {str(e)}")
                error_count += 1
        print(f"== success_count: {success_count}, error_count: {error_count} ==")

    finally:
        print("== Stopping Spark session...")
        spark.stop()
        print("== Spark session stopped")

if __name__ == "__main__":
    create_tables()

