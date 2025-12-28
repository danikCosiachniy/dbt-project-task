from __future__ import annotations

from datetime import datetime
from typing import Any

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from utils.constants import DBT_PROJECT_PATH
from utils.dbt_logger import (
    log_dag_success_callback,
    log_failure_callback,
    log_start_callback,
    log_success_callback,
)
from utils.get_creeds import get_env
from utils.load_mode import should_full_refresh

# dbt profile configuration (points to profiles.yml inside the project)
profile_config: ProfileConfig = ProfileConfig(
    profile_name='retail_vault',
    target_name='snowflake',
    profiles_yml_filepath=f'{DBT_PROJECT_PATH}/profiles.yml',
)

# Default arguments applied to all tasks in the DAG
default_args: dict[str, Any] = {
    'owner': 'airflow',
    'on_failure_callback': log_failure_callback,
    'on_success_callback': log_success_callback,
    'on_execute_callback': log_start_callback,
}


def decide_full_refresh() -> bool:
    """
    Decide full_refresh at DAGRun runtime.

    Priority:
      1) dag_run.conf["full_refresh"] if provided (manual/makefile trigger)
      2) auto-detection via should_full_refresh() (Snowflake state)
    """
    ctx = get_current_context()
    conf = (ctx.get('dag_run').conf or {}) if ctx.get('dag_run') else {}

    if 'full_refresh' in conf:
        return bool(conf['full_refresh'])

    # auto mode (anchor table in RAW_VAULT schema)
    return bool(should_full_refresh(anchor_table='HUB_CUSTOMER'))


# Main Airflow DAG definition
with DAG(
    dag_id='retail_vault_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    on_success_callback=log_dag_success_callback,
    tags=['dbt', 'retail', 'snowflake'],
    render_template_as_native_obj=True,
) as dag:
    # Fetch credentials before creating tasks
    dbt_env = get_env()
    # Decide full_refresh at runtime and push to XCom
    decide_mode = PythonOperator(
        task_id='decide_full_refresh',
        python_callable=decide_full_refresh,
    )
    # Common operator arguments applied to all dbt tasks
    common_operator_args = {
        'install_deps': True,  # Automatically runs `dbt deps` once
        'full_refresh': "{{ ti.xcom_pull(task_ids='decide_full_refresh') }}",  # Uses incremental logic where defined
        'env': dbt_env,
    }
    dbt_seed = DbtTaskGroup(
        group_id='dbt_seed',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            # Only resources of type "seed" (customer_master.csv, etc.)
            select=['resource_type:seed'],
            env_vars=dbt_env,
        ),
        operator_args=common_operator_args,
    )
    # STAGING (views)
    staging_tg = DbtTaskGroup(
        group_id='staging',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            select=['staging.*'],  # only models under models/staging/**
            env_vars=dbt_env,
        ),
        operator_args=common_operator_args,
    )
    # RAW VAULT (hubs / links / sats)
    raw_vault_tg = DbtTaskGroup(
        group_id='raw_vault',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            select=['raw_vault.*'],  # only models under models/raw_vault/**
            env_vars=dbt_env,
        ),
        operator_args=common_operator_args,
    )
    # BUSINESS VAULT
    business_vault_tg = DbtTaskGroup(
        group_id='business_vault',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            select=['business_vault.*'],  # only models under models/buisness_vault/**
            env_vars=dbt_env,
        ),
        operator_args=common_operator_args,
    )
    # MARTS (dimensions + facts)
    marts_tg = DbtTaskGroup(
        group_id='marts',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            select=['marts.*'],  # only models under models/marts/**
            env_vars=dbt_env,
        ),
        operator_args=common_operator_args,
    )
    # Execution order:
    decide_mode >> staging_tg >> raw_vault_tg >> dbt_seed >> business_vault_tg >> marts_tg
