from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="orchestration_dbt_databricks",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["databricks", "dbt"]
) as dag:

    create_schema = DatabricksRunNowOperator(
        task_id="create_schema",
        databricks_conn_id="databricks_default",
        job_id=171739688557399
    )

    raw_to_bronze = DatabricksRunNowOperator(
        task_id="raw_to_bronze",
        databricks_conn_id="databricks_default",
        job_id=811991280842644
    )

    address = DatabricksRunNowOperator(
        task_id="address",
        databricks_conn_id="databricks_default",
        job_id=1006089901927135
    )

    customer = DatabricksRunNowOperator(
        task_id="customer",
        databricks_conn_id="databricks_default",
        job_id=436490013992375
    )

    cust_address = DatabricksRunNowOperator(
        task_id="cust_address",
        databricks_conn_id="databricks_default",
        job_id=4503319305606
    )

    product = DatabricksRunNowOperator(
        task_id="product",
        databricks_conn_id="databricks_default",
        job_id=545966284593835
    )

    product_category = DatabricksRunNowOperator(
        task_id="product_category",
        databricks_conn_id="databricks_default",
        job_id=993372062147846
    )

    product_description = DatabricksRunNowOperator(
        task_id="product_description",
        databricks_conn_id="databricks_default",
        job_id=389787984491487
    )

    product_model = DatabricksRunNowOperator(
        task_id="product_model",
        databricks_conn_id="databricks_default",
        job_id=230972610884036
    )

    product_model_desc = DatabricksRunNowOperator(
        task_id="product_model_desc",
        databricks_conn_id="databricks_default",
        job_id=1121118559641428
    )

    sales_order_detail = DatabricksRunNowOperator(
        task_id="sales_order_detail",
        databricks_conn_id="databricks_default",
        job_id=442871237886136
    )

    sales_order_header = DatabricksRunNowOperator(
        task_id="sales_order_header",
        databricks_conn_id="databricks_default",
        job_id=1097062602540644
    )

    vget_all_cat = DatabricksRunNowOperator(
        task_id="vget_all_cat",
        databricks_conn_id="databricks_default",
        job_id=118245369449002
    )

    vproduct_description = DatabricksRunNowOperator(
        task_id="vproduct_description",
        databricks_conn_id="databricks_default",
        job_id=962171798397401
    )

    vproduct_model_cat_desc = DatabricksRunNowOperator(
        task_id="vproduct_model_cat_desc",
        databricks_conn_id="databricks_default",
        job_id=409613578078301
    )
    
    create_schema >> raw_to_bronze >> [address, customer, cust_address, product, product_category, product_description, product_model, product_model_desc, sales_order_detail, sales_order_header, vget_all_cat, vproduct_description, vproduct_model_cat_desc]