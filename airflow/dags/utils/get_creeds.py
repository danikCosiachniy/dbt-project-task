from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.constants import CONN_NAME


def get_env() -> dict[str, str]:
    """
    Extract Snowflake credentials from Airflow connection
    and expose them as environment variables for dbt.
    """
    hook = SnowflakeHook(CONN_NAME)
    conn = hook.get_connection(hook.snowflake_conn_id)

    extra = conn.extra_dejson or {}

    return {
        'SNOWFLAKE_ACCOUNT': conn.host or extra.get('account'),
        'SNOWFLAKE_USER': conn.login,
        'SNOWFLAKE_PASSWORD': conn.password,  # nosec
        'SNOWFLAKE_ROLE': extra.get('role'),
        'SNOWFLAKE_WAREHOUSE': extra.get('warehouse'),
        'SNOWFLAKE_DATABASE': extra.get('database'),
        'SNOWFLAKE_SCHEMA': extra.get('schema') or conn.schema,
    }
