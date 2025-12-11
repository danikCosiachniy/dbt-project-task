from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Literal, cast

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from airflow import DAG
from airflow.models import Variable
from utils.constants import DBT_PROJECT_PATH, VAR_NAME
from utils.dbt_logger import log_failure_callback, log_start_callback, log_success_callback

# Allowed dbt targets configured in profiles.yml
DbtTarget = Literal['snowflake', 'dev']


def get_dbt_target_from_env() -> DbtTarget:
    """
    Read dbt target from the DBT_TARGET env var.

    Valid values:
      - "snowflake" -> run against Snowflake
      - "dev"       -> run against DuckDB

    Defaults to "snowflake" if not set or invalid.
    """
    value = os.getenv('DBT_TARGET', 'snowflake').lower()
    if value not in ('snowflake', 'dev'):
        return 'snowflake'
    return cast(DbtTarget, value)


def get_env(target: DbtTarget) -> dict[str, str]:
    """
    Build env dict for dbt run, depending on target.

    - dev (DuckDB): no extra env needed → {}
    - snowflake: read creds from Airflow Variable <var_name> (JSON) and map to SNOWFLAKE_*.
    """
    if target == 'dev':
        # DuckDB uses a local file path only (no credentials)
        return {}
    if target == 'snowflake':
        try:
            config = Variable.get(VAR_NAME, deserialize_json=True)

            return {
                'SNOWFLAKE_ACCOUNT': config.get('account'),
                'SNOWFLAKE_USER': config.get('user'),
                'SNOWFLAKE_PASSWORD': config.get('password'),  # nosec
                'SNOWFLAKE_ROLE': config.get('role'),
                'SNOWFLAKE_WAREHOUSE': config.get('warehouse'),
                'SNOWFLAKE_DATABASE': config.get('database'),
                'SNOWFLAKE_SCHEMA': config.get('schema'),
            }
        except (KeyError, ValueError) as e:
            print(f'Warning: Could not fetch variable {VAR_NAME}: {e}')
            return {}
    return {}


# Resolve which dbt target to use (Snowflake or DuckDB) from env
DBT_TARGET: DbtTarget = get_dbt_target_from_env()

# dbt profile configuration (points to profiles.yml inside the project)
profile_config: ProfileConfig = ProfileConfig(
    profile_name='retail_vault',
    target_name=DBT_TARGET,
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
    tags=['dbt', 'retail', DBT_TARGET],
) as dag:
    # Fetch credentials before creating tasks
    dbt_env = get_env(DBT_TARGET)
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
        ),
        operator_args=common_operator_args,
    )
    # Execution order:
    staging_tg >> dbt_seed >> raw_vault_tg >> buisness_vault_tg >> marts_tg
