# Airflow Orchestration

This directory contains **Apache Airflow** configuration and DAGs used to orchestrate the dbt pipeline.
The project uses **Astronomer Cosmos** to automatically translate the dbt project into Airflow tasks.

---

## 📁 Structure

- **`dags/retail_pipeline.py`**
  Main Airflow DAG.
  Uses `ProjectConfig`, `ExecutionConfig`, and `ProfileConfig` from **Cosmos** to execute dbt models inside the same Docker environment as Airflow.

- **`dags/utils/`**
  Helper modules:
  - logging
  - Telegram notifications
  - dbt runner wrappers
  - failure callbacks

- **`plugins/`**
  Custom Airflow plugins (optional, currently not required).

- **`logs/`**
  Airflow execution logs.
  Mounted as a Docker volume and excluded from Git.

---

## ⚙️ How it works

1. **Docker image build**
   - Python dependencies are installed from `pyproject.toml` (via `uv → requirements.txt`).
   - Includes `dbt-core`, `dbt-snowflake`, `astronomer-cosmos`, and Airflow providers.

2. **DAG initialization**
   - Airflow loads `retail_pipeline.py`.
   - Cosmos reads the dbt project configuration.

3. **dbt project discovery**
   - The dbt project directory is resolved inside the container.
   - Profiles are loaded from `profiles.yml`.
   - Snowflake credentials are taken from the Airflow connection `snowflake_default`.

4. **Task graph generation**
   - Cosmos parses `dbt_project.yml`.
   - dbt models are converted into Airflow tasks automatically.
   - Dependencies between models are preserved.

5. **Execution**
   - Tasks run locally inside the Airflow worker container.
   - dbt is executed via `dbtRunner` or custom dbt runner utilities.
   - Logs are written to the Airflow logs directory.

---

## 🔐 Credentials & Secrets

- **Snowflake credentials**
  Stored in **Airflow Connections** (`snowflake_default`).

- **Telegram credentials**
  Stored in **Airflow Variables** (JSON format).

No warehouse credentials or tokens are stored in the repository or `.env`.

---

## 📝 Notes

- The DAG does **not** rely on `.env` for warehouse access.
- All runtime secrets are managed via Airflow.
- The DAG is environment-agnostic and can run locally or in CI/CD with the same configuration.
