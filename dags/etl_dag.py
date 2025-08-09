from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 8),
    "retries": 1,
}

with DAG(
    dag_id='etl_crm_erp',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    extract_to_bronze = BashOperator(
        task_id="extract_from_db_to_bronze",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/bronze/extract.py"
    )

    transform_crm_cust_info = BashOperator(
        task_id="transform_crm_cust_info_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_crm_cust_info.py"
    )

    transform_crm_prd_info = BashOperator(
        task_id="transform_crm_prd_info_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_crm_prd_info.py"
    )

    transform_crm_sales_details = BashOperator(
        task_id="transform_crm_sales_details_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_crm_sales_details.py"
    )

    transform_erp_cust_az12 = BashOperator(
        task_id="transform_erp_cust_az12_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_erp_cust_az12.py"
    )

    transform_erp_loc_a101 = BashOperator(
        task_id="transform_erp_loc_a101_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_erp_loc_a101.py"
    )

    transform_erp_px_cat_g1v2 = BashOperator(
        task_id="transform_erp_px_cat_g1v2_to_silver",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/silver/transform_erp_px_cat_g1v2.py"
    )

    view_silver_to_gold = BashOperator(
        task_id="view_silver_to_gold",
        bash_command="sudo -u hiveuser /usr/local/spark/bin/spark-submit /home/ldduc/D/data-warehouse-apache-ecosystem/src/gold/view_gold.py"
    )

    extract_to_bronze >> [
        transform_crm_cust_info,
        transform_crm_prd_info,
        transform_crm_sales_details,
        transform_erp_cust_az12,
        transform_erp_loc_a101,
        transform_erp_px_cat_g1v2,
    ] >> view_silver_to_gold
