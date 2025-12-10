# Airflow Orchestration

This directory contains the DAG configurations. We use the **Astronomer Cosmos** library to automatically render the dbt project as tasks within Airflow.

## Structure

- **`dags/retail_pipeline.py`**: The main DAG. Uses `ExecutionConfig` and `ProjectConfig` from Cosmos to run dbt models in the same Docker environment.
- **`dags/utils/`**: Auxiliary scripts (loggers and utilities).
- **`plugins/`**: Custom Airflow plugins (if required).
- **`logs/`**: Execution logs (mapped from the container, added to `.gitignore`).

## How it works

1. The Docker container installs dependencies from `pyproject.toml` (including `dbt-core`, `dbt-duckdb`, etc.) into the system Python.
2. Airflow triggers the DAG.
3. Cosmos locates the `dbt_project` folder (via the `AIRFLOW_VAR_DBT_PROJECT_DIR` variable) and builds the task graph based on `dbt_project.yml`.
4. Tasks are executed locally within the Airflow worker.
