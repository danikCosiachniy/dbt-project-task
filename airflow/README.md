# Airflow Orchestration

This directory contains **Apache Airflow** configuration and DAGs used to orchestrate the dbt pipeline.
The project uses **Astronomer Cosmos** to automatically translate the dbt project into Airflow tasks.

---

## 📁 Structure

- **`dags/retail_vault_dag.py`**
  Main load DAG (Cosmos).
  Uses branching to choose **initial** vs **incremental** chain based on Snowflake state (and can be overridden via DAG run config).

- **`dags/cleanup_database.py`**
  Maintenance DAG.
  Drops Snowflake schemas by prefix (used to reset the environment).

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
   - Python dependencies are installed from `pyproject.toml` using `uv`.
   - The image includes `dbt-core`, `dbt-snowflake`, `astronomer-cosmos`, and required Airflow providers.

2. **DAGs loaded by Airflow**
   - `retail_vault_dag` — load DAG (Cosmos)
   - `cleanup_database` — maintenance DAG

3. **Credentials & configuration**
   - dbt profile is read from `profiles.yml` inside the dbt project.
   - Snowflake credentials are taken from the Airflow connection **`snowflake_default`**.
   - `.env` is only for local container/Airflow configuration (not warehouse secrets).

4. **Task graph generation (Cosmos)**
   - Cosmos parses `dbt_project.yml`.
   - dbt models are converted into Airflow tasks.
   - Dependencies between models are preserved.

5. **Load mode selection**
   - In **auto** mode, the DAG checks Snowflake state (anchor table in `<BASE_SCHEMA>_RAW_VAULT`).
   - You can **force** the mode via DAG run config: `{"mode": "initial"}` or `{"mode": "incremental"}`.

6. **Execution**
   - Tasks execute inside the running Airflow container.
   - Logs are written to the Airflow logs directory.

---

## 🔐 Credentials & Secrets

- **Snowflake credentials**
  Stored in **Airflow Connections** (`snowflake_default`).

- **Telegram credentials**
  Stored in **Airflow Variables** (JSON format).

No warehouse credentials or tokens are stored in the repository. `.env` is used only for local container/Airflow settings.

---

## ▶️ Triggering DAGs (local)

From the repository root, use the Makefile targets:

```bash
make auto-load          # triggers retail_vault_dag (auto initial vs incremental)
make initial-load       # triggers retail_vault_dag with {"mode":"initial"}
make incremental-load   # triggers retail_vault_dag with {"mode":"incremental"}
make clean-up           # triggers cleanup_database
```

These targets run `airflow dags trigger ...` inside the running container.

---

## 📝 Notes

- Load DAG supports **auto** selection and **manual override** via run config (used by Makefile targets).
- All runtime secrets are managed via Airflow.
- The DAG is environment-agnostic and can run locally or in CI/CD with the same configuration.
