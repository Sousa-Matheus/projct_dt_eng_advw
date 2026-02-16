from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
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

    silver_pipeline = DatabricksRunNowOperator(
        task_id="silver_pipeline",
        databricks_conn_id="databricks_default",
        job_id=1023486146196329
    )

    gold_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt dbt run"
    )

    
    create_schema >> raw_to_bronze >> silver_pipeline >> gold_dbt_run