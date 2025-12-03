import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Пути ВНУТРИ Docker контейнера
DBT_ROOT_PATH = "/opt/airflow/dbt_project"

# Конфигурация профиля
# Мы говорим Cosmos'у использовать переменные окружения (SNOWFLAKE_ACCOUNT и т.д.)
# для подключения. Это безопаснее, чем хардкодить пароли.
profile_config = ProfileConfig(
    profile_name="retail_vault_profile",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="retail_vault_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "retail"],
) as dag:

    # Используем DbtTaskGroup (группа задач внутри DAG)
    dbt_processing = DbtTaskGroup(
        group_id="dbt_core_logic",  # Уникальное имя группы
        
        # Конфиг проекта
        project_config=ProjectConfig(DBT_ROOT_PATH),
        
        # Конфиг профиля
        profile_config=profile_config,
        
        # Настройки запуска
        operator_args={
            "install_deps": True,   # Автоматически запустит dbt deps
            "full_refresh": False,  # Можно менять через UI
        },
        
        # Указываем путь к dbt (он в PATH, но лучше явно)
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
    )

    # Здесь можно добавить другие задачи, например:
    # send_slack_alert >> dbt_processing