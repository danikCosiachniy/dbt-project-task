# Absolute path to the mounted dbt project inside the Airflow container
DBT_PROJECT_PATH: str = '/opt/airflow/dbt_project'
# Path to the central dbt/Airflow log file inside the container
LOG_FILE_PATH: str = '/opt/airflow/logs/dbt_pipeline.log'
# The name of connection in Airflow with your creeds
CONN_NAME: str = 'snowflake_default'
# The name of variable with tg creeds
VAR_TG_NAME = 'tg_secrets'
