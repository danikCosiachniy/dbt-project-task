from __future__ import annotations

from datetime import datetime
from typing import Any

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
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
    # Branching
    check_state = BranchPythonOperator(
        task_id='check_db_state',
        python_callable=should_full_refresh,
    )
    with TaskGroup(group_id='incremental_chain') as incremental_chain:
        # Start point of incremental-chain
        start = EmptyOperator(task_id='start')
        # Common operator arguments applied to all dbt tasks
        common_operator_args_incr = {
            'install_deps': True,
            'full_refresh': False,
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
            operator_args=common_operator_args_incr,
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
            operator_args=common_operator_args_incr,
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
            operator_args=common_operator_args_incr,
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
            operator_args=common_operator_args_incr,
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
            operator_args=common_operator_args_incr,
        )
        # Execution order:
        start >> staging_tg >> raw_vault_tg >> dbt_seed >> business_vault_tg >> marts_tg
    with TaskGroup(group_id='initial_chain') as initial_chain:
        # Start point of initial-chain
        start = EmptyOperator(task_id='start')
        # Common operator arguments applied to all dbt tasks
        common_args_init = {
            'install_deps': True,
            'full_refresh': True,
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
            operator_args=common_args_init,
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
            operator_args=common_args_init,
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
            operator_args=common_args_init,
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
            operator_args=common_args_init,
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
            operator_args=common_args_init,
        )
        # Execution order:
        start >> staging_tg >> raw_vault_tg >> dbt_seed >> business_vault_tg >> marts_tg

    check_state >> [incremental_chain, initial_chain]
