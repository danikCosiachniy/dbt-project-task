from __future__ import annotations

from datetime import datetime
from typing import Any

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from airflow import DAG
from utils.constants import DBT_PROJECT_PATH
from utils.dbt_logger import (
    log_dag_success_callback,
    log_failure_callback,
    log_start_callback,
    log_success_callback,
)
from utils.get_creeds import get_env

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

# Main Airflow DAG definition
with DAG(
    dag_id='retail_vault_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    on_success_callback=log_dag_success_callback,
    tags=['dbt', 'retail', 'snowflake'],
) as dag:
    # Fetch credentials before creating tasks
    dbt_env = get_env()
    # Common operator arguments applied to all dbt tasks
    common_operator_args = {
        'install_deps': True,  # Automatically runs `dbt deps` once
        'full_refresh': False,  # Uses incremental logic where defined
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
    buisness_vault_tg = DbtTaskGroup(
        group_id='buisness_vault',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path='dbt'),
        render_config=RenderConfig(
            select=['buisness_vault.*'],  # only models under models/buisness_vault/**
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
    staging_tg >> raw_vault_tg >> dbt_seed >> buisness_vault_tg >> marts_tg
