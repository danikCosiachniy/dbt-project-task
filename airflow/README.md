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
   - Airflow loads `dags/retail_pipeline.py`.
   - Cosmos reads the dbt project configuration and renders dbt models as Airflow tasks.

3. **Credentials & Snowflake connectivity**
   - Snowflake credentials are stored in Airflow Connections (default: `snowflake_default`).
   - The helper `utils/get_creeds.py` exposes the connection values as env vars for dbt.

4. **Load mode detection (initial vs incremental)**
   - The pipeline does **not** rely on an Airflow Variable to decide the mode.
   - Instead, it checks Snowflake at runtime via `SnowflakeHook`:
     - If the anchor table does not exist (or is empty) → **initial-load** (`--full-refresh`).
     - If the anchor table exists and has rows → **incremental**.
   - The logic lives in `dags/utils/load_mode.py` and is used to set `full_refresh` for Cosmos operators.

5. **Execution**
   - Tasks run locally inside the Airflow worker container.
   - dbt is executed via Cosmos (`dbtRunner`).
   - Logs are written to the Airflow logs directory.

---

## 🔐 Credentials & Secrets

- **Snowflake credentials**
  Stored in **Airflow Connections** (`snowflake_default`).

- **Telegram credentials**
  Stored in **Airflow Variables** (JSON format).

No warehouse credentials or tokens are stored in the repository or `.env`.

---

## 🧠 Initial vs Incremental logic

The project distinguishes **initial-load** vs **incremental** runs using the actual state of Snowflake.

- **Anchor schema**: derived from the base schema in the connection (e.g. `DBT_VAULT`) and a suffix.
  - Example: `DBT_VAULT` → `DBT_VAULT_RAW_VAULT`
- **Anchor table**: by default `HUB_CUSTOMER` in the anchor schema.

The helper `should_full_refresh()` performs two checks:

1. **Table existence** via `INFORMATION_SCHEMA.TABLES`
2. **Data presence** via `SELECT 1 ... LIMIT 1`

Result:
- `full_refresh=True` → initial-load (`dbt --full-refresh`)
- `full_refresh=False` → incremental

If the Snowflake check fails, the implementation is intentionally conservative and defaults to **incremental** to avoid accidental destructive rebuilds.

---

## 📝 Notes

- The DAG does **not** rely on `.env` for warehouse access.
- All runtime secrets are managed via Airflow.
- The DAG is environment-agnostic and can run locally or in CI/CD with the same configuration.
- A separate maintenance DAG (`cleanup_database`) can be used to drop dbt-created schemas (CASCADE) and reset the initialization state when you want a clean re-run.
