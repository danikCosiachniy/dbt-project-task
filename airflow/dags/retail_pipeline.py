import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from utils.dbt_logger import log_success_callback, log_failure_callback, log_start_callback

DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

profile_config = ProfileConfig(
    profile_name="retail_vault_profile",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml"
)

default_args = {
    "owner": "airflow",
    "on_failure_callback": log_failure_callback,
    "on_success_callback": log_success_callback,
    "on_execute_callback": log_start_callback
}

with DAG(
    dag_id="retail_vault_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "retail", "duckdb"],
) as dag:

    dbt_processing = DbtTaskGroup(
        group_id="dbt_core_logic",
        
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        
        operator_args={
            "install_deps": True,
            "full_refresh": False,
        },
    )