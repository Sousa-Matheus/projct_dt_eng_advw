from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
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

    bronze_to_silver = DatabricksRunNowOperator(
        task_id="bronze_to_silver",
        databricks_conn_id="databricks_default",
        job_id=123456789
    )

    silver_to_gold_dbt = BashOperator(
        task_id="silver_to_gold_dbt",
        bash_command="cd /usr/app/dbt && dbt run --models gold"
    )

    bronze_to_silver >> silver_to_gold_dbt
