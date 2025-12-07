### 2. Файл `airflow/README.md`

```markdown
# Airflow Orchestration

В этой директории содержится конфигурация DAG-ов. Мы используем библиотеку **Astronomer Cosmos**, чтобы автоматически рендерить dbt-проект как задачи внутри Airflow.

## Структура
- **`dags/retail_pipeline.py`**: Основной DAG. Использует `ExecutionConfig` и `ProjectConfig` из Cosmos для запуска dbt-моделей в том же окружении Docker.
- **`dags/utils/`**: Вспомогательные скрипты (логгеры и утилиты).
- **`plugins/`**: Кастомные плагины Airflow (если потребуются).
- **`logs/`**: Логи выполнения (пробрасываются из контейнера, добавлены в `.gitignore`).

## Как это работает
1. Docker контейнер устанавливает зависимости из `pyproject.toml` (включая `dbt-core`, `dbt-duckdb` и т.д.) в системный Python.
2. Airflow запускает DAG.
3. Cosmos находит папку `dbt_project` (через переменную `AIRFLOW_VAR_DBT_PROJECT_DIR`) и строит граф задач на основе `dbt_project.yml`.
4. Задачи выполняются локально внутри воркера Airflow.
