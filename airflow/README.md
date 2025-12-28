# Airflow Orchestration

This directory contains **Apache Airflow** configuration and DAGs used to orchestrate the dbt pipeline.
The project uses **Astronomer Cosmos** to automatically translate the dbt project into Airflow tasks.

---

## 📁 Structure

- **`dags/retail_vault_initial_dag.py`**
  Full-load DAG (Cosmos). Runs the pipeline with `full_refresh=True`.

- **`dags/retail_vault_incremental_dag.py`**
  Incremental-load DAG (Cosmos). Runs the pipeline with `full_refresh=False`.

- **`dags/cleanup_database.py`**
  Maintenance DAG. Drops Snowflake schemas by prefix (used to reset the environment).

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
   - Airflow loads three DAGs:
     - `retail_vault_initial_dag` (full refresh)
     - `retail_vault_incremental_dag` (incremental)
     - `cleanup_database` (maintenance)

3. **dbt project discovery**
   - The dbt project directory is resolved inside the container.
   - Profiles are loaded from `profiles.yml`.
   - Snowflake credentials are taken from the Airflow connection `snowflake_default`.

4. **Task graph generation (Cosmos)**
   - Cosmos parses `dbt_project.yml`.
   - dbt models are converted into Airflow tasks.
   - Dependencies between models are preserved.

5. **Execution**
   - Tasks run locally inside the same Docker container.
   - dbt is executed via Cosmos operators.
   - Logs are written to the Airflow logs directory.

---

## 🔐 Credentials & Secrets

- **Snowflake credentials**
  Stored in **Airflow Connections** (`snowflake_default`).

- **Telegram credentials**
  Stored in **Airflow Variables** (JSON format).

No warehouse credentials or tokens are stored in the repository or `.env`.

---

## ▶️ Triggering DAGs (local)

From the repository root, use the Makefile targets:

```bash
make initial-load       # triggers retail_vault_initial_dag
make incremental-load   # triggers retail_vault_incremental_dag
make clean-up           # triggers cleanup_database
```

These targets run `airflow dags trigger ...` inside the running container.

---

## 📝 Notes

- DAGs do **not** rely on `.env` for warehouse access (only for local container/Airflow config).
- All runtime secrets are managed via Airflow.
- The DAG is environment-agnostic and can run locally or in CI/CD with the same configuration.
