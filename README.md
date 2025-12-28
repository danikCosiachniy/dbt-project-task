# Retail Data Vault Pipeline

This repository contains an ETL/ELT pipeline for retail data processing built using the **Data Vault 2.0** methodology.
Transformations are implemented with **dbt**, orchestration is handled by **Apache Airflow** using **Astronomer Cosmos** for native dbt integration.

The project is designed to work with **Snowflake** as the analytical warehouse and follows a layered architecture:
**staging → raw vault → business vault → marts**.

---

## 🛠 Tech Stack

- **Orchestration:** Apache Airflow 2.10+ (Cosmos)
- **Transformations:** dbt Core 1.7+
- **Data Warehouse:** Snowflake
- **Metadata & Orchestration DB:** Postgres (runs inside the Airflow container)
- **Dependency Management:** uv (Astral)
- **Infrastructure:** Docker (single-container local setup)
- **Methodology:** Data Vault 2.0

---

## 🔐 Credentials & Secrets Management

⚠️ **Important:** Warehouse credentials are **not stored in `.env`**.

### Telegram notifications

Telegram credentials are stored in **Airflow Variables** as a single JSON object.

**Variable name:** `telegram_credentials`

```json
{
  "bot_token": "<BOT_TOKEN>",
  "chat_id": "<CHAT_ID>"
}
```
### ❄️ Snowflake credentials

Snowflake credentials must be stored in **Airflow Connections**.

- **Connection ID:** `snowflake_default`
- **Connection Type:** `Snowflake`

**Required fields:**
- `Account`
- `User`
- `Password` / `Key`
- `Role`
- `Warehouse`
- `Database`
- `Schema`

> Both **dbt** and **Airflow** rely on this connection via Cosmos and custom dbt runners.

---

# 🚀 Quick Start

## 1) Environment Configuration (`.env`)

Create a `.env` file in the root directory.

**Important:** This file is used **only** for Airflow/Docker infrastructure configuration. It is **not** used for warehouse credentials.

```bash
touch .env
```

Example `.env`:

```ini
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow

POSTGRES_USER=airflow
POSTGRES_DB=airflow
POSTGRES_PASSWORD=airflow
PGDATA=/var/lib/postgresql/data
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

---

## 2) Build & Run 🛠️

The project is managed via **Makefile** (recommended approach).

**Build image**
```bash
make build
```

**Run container**
```bash
make up
```

**Full rebuild (rebuild + restart)**
```bash
make rebuild
```

---

## 3) Access 🖥️

- **Airflow UI:** http://localhost:8080
- **Login / Password:** sourced from `.env` (example: `admin / admin`)

---

## ▶️ Running dbt Pipelines

All dbt commands are executed **inside the Airflow container** using a custom runner.

**Full load** (deps + seeds + full-refresh build)
```bash
make initial-load
```

**Incremental load**
```bash
make incremental-load
```

**Clean artifacts**
```bash
make clean-up
```

---

## 🧪 Linting & Quality Checks

All linters are executed via pre-commit.

```bash
make lint
```

This includes:
- Python linting/formatting (`ruff`)
- SQL linting (`sqlfluff`)
- YAML & whitespace checks

---

## 📦 Dependency Management (uv)

Dependencies are managed with `uv`.

**Add a new Python dependency:**

1. Edit `pyproject.toml`
2. Regenerate lockfile:
   ```bash
   uv lock
   ```
3. Rebuild image:
   ```bash
   make rebuild
   ```

This guarantees consistent versions across:
- Airflow
- dbt
- Cosmos
- Local development

---

## 📂 Project Structure

```text
.
├── airflow/                         # Airflow-specific code and configuration
│   ├── dags/
│   │   ├── retail_pipeline.py       # Main DAG (Cosmos-based dbt orchestration)
│   │   └── utils/                   # Helpers (dbt runner, notifications, callbacks)
│   ├── logs/                        # Airflow logs (mounted)
│   ├── plugins/                     # Optional custom Airflow plugins
│   └── README.md                    # Airflow-specific documentation
│
├── dbt_vault_retail/                # dbt project root
│   ├── models/
│   │   ├── staging/                 # Source-aligned staging models
│   │   ├── raw_vault/               # Hubs, Links, Satellites
│   │   ├── business_vault/          # PITs, effectivity sats, business sats
│   │   └── marts/                   # Dimensions and facts
│   ├── macros/                      # Shared dbt macros
│   ├── seeds/                       # Seed data (e.g. customer_master)
│   ├── snapshots/                   # dbt snapshots (optional)
│   ├── profiles.yml                 # dbt profile (uses Airflow connection)
│   ├── dbt_project.yml              # dbt project configuration
│   └── README.md                    # dbt-specific documentation
│
├── docker/
│   └── entrypoint.sh                # Starts Postgres + runs Airflow migrations + starts scheduler/webserver
├── Dockerfile                       # Custom Airflow image with dbt & Cosmos deps
├── Makefile                         # Project commands (build, run, lint, dbt runs)
├── pyproject.toml                   # Python dependencies (uv / PEP 621)
├── uv.lock                          # Dependency lockfile
└── README.md                        # Root documentation (this file)
```

---

## 📌 Notes

- Warehouse credentials are **never** stored in code or `.env`.
- All dbt models follow **Data Vault 2.0** best practices.
- Facts and dimensions are built only from Vault layers, never directly from staging.
- PIT tables provide historical “as-of” business views.

---

## 📎 Related Documentation

- `airflow/README.md` — Airflow DAGs & orchestration details
- `dbt_vault_retail/README.md` — Data Vault & dbt architecture
