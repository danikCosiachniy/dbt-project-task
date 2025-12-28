from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.constants import CONN_NAME
from utils.dbt_logger import log_failure_callback, log_start_callback, log_success_callback
from utils.get_creeds import get_env


def _qi(name: str) -> str:
    """Quote identifier for Snowflake (safe for case/special chars)."""
    return '"' + name.replace('"', '""') + '"'


def _qs(value: str) -> str:
    """Quote SQL string literal safely."""
    return "'" + value.replace("'", "''") + "'"


def _list_schemas_with_prefix(hook: SnowflakeHook, database: str, prefix: str) -> list[str]:
    db = _qi(database)
    pfx = prefix.upper()

    sql = f"""
    SELECT schema_name
    FROM {db}.INFORMATION_SCHEMA.SCHEMATA
    WHERE LEFT(UPPER(schema_name), LENGTH({_qs(pfx)})) = {_qs(pfx)}
    ORDER BY schema_name
    """
    rows = hook.get_records(sql)
    return [r[0] for r in rows]


def delete_all_schemas() -> None:
    """
    Drop base schema and all schemas that start with the base schema prefix (CASCADE).
    Uses SnowflakeHook (Airflow Connection).
    """
    env = get_env()
    database = env.get('SNOWFLAKE_DATABASE')
    base_schema = env.get('SNOWFLAKE_SCHEMA')
    hook = SnowflakeHook(CONN_NAME)

    schemas_to_drop = _list_schemas_with_prefix(hook, database=database, prefix=base_schema)

    db = _qi(database)
    for sch in schemas_to_drop:
        sql = f'DROP SCHEMA IF EXISTS {db}.{_qi(sch)} CASCADE'
        print(f'Executing: {sql}')
        hook.run(sql)


default_args: dict[str, Any] = {
    'owner': 'airflow',
    'on_execute_callback': log_start_callback,
    'on_success_callback': log_success_callback,
    'on_failure_callback': log_failure_callback,
    'retries': 0,
}

with DAG(
    dag_id='cleanup_database',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['maintenance', 'cleanup', 'snowflake'],
    max_active_runs=1,
) as dag:
    cleanup_task = PythonOperator(
        task_id='drop_dbt_schemas_by_prefix',
        python_callable=delete_all_schemas,
    )
