from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.constants import CONN_NAME
from utils.get_creeds import get_env


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def should_full_refresh(anchor_table: str = 'HUB_CUSTOMER') -> str:
    """Return the branch task_id based on Snowflake state.

    This helper is intended for ``BranchPythonOperator``.

    Decision rules (anchor table: ``HUB_CUSTOMER`` in schema ``<BASE_SCHEMA>_RAW_VAULT``):
      - If the anchor table does not exist -> return ``"initial_chain.start"``
      - If the anchor table exists but has 0 rows -> return ``"initial_chain.start"``
      - If the anchor table exists and has rows -> return ``"incremental_chain.start"``

    Notes:
      - ``SNOWFLAKE_DATABASE`` and ``SNOWFLAKE_SCHEMA`` are taken from ``get_env()``.
      - Any Snowflake/connection errors will propagate (no internal try/except).

    :param anchor_table: Anchor table name used to detect whether the Vault is initialized.
    :return: The downstream task_id to follow.
    :rtype: str
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
        return 'initial_chain.start'

    # 2) table has rows?
    has_rows_sql = f"""
    SELECT 1
    FROM {_qi(database)}.{_qi(schema)}.{_qi(anchor_table)}
    LIMIT 1
    """
    rows = hook.get_records(has_rows_sql)
    if len(rows) == 0:
        return 'initial_chain.start'
    return 'incremental_chain.start'
