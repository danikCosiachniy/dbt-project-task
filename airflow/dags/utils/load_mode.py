from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.constants import CONN_NAME
from utils.get_creeds import get_env


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def should_full_refresh(anchor_table: str = 'HUB_CUSTOMER') -> bool:
    """
    Decide whether to run dbt with --full-refresh based on Snowflake state.

    Rules:
      - If the anchor table does not exist -> True (initial-load)
      - If the anchor table exists but has 0 rows -> True (initial-load)
      - If the anchor table exists and has rows -> False (incremental)

    NOTE: If connection/query fails, returns False (incremental) to avoid accidental full-refresh.
    """
    env = get_env()
    database = env.get('SNOWFLAKE_DATABASE')
    base_schema = env.get('SNOWFLAKE_SCHEMA')

    if not database or not base_schema:
        raise ValueError('SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA must be set')

    # Build the target schema as "<BASE_SCHEMA>_RAW_VAULT"
    schema = f'{base_schema}_RAW_VAULT'

    hook = SnowflakeHook(CONN_NAME)
    # 1) table exists?
    exists_sql = f"""
    SELECT 1
    FROM {_qi(database)}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = %s
      AND table_name   = %s
    LIMIT 1
    """
    exists = hook.get_records(
        exists_sql,
        parameters=(schema.upper(), anchor_table.upper()),
    )
    if not exists:
        return True

    # 2) table has rows?
    has_rows_sql = f"""
    SELECT 1
    FROM {_qi(database)}.{_qi(schema)}.{_qi(anchor_table)}
    LIMIT 1
    """
    rows = hook.get_records(has_rows_sql)
    return len(rows) == 0
