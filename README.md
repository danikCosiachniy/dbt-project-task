# Retail Data Vault Pipeline

This project is an ETL/ELT pipeline for retail data processing. It implements **Data Vault 2.0** methodology using **dbt** for transformations and **Apache Airflow** (via Astronomer Cosmos) for orchestration.

## 🛠 Tech Stack

- **Orchestration:** Apache Airflow 2.10+
- **Transformation:** dbt Core 1.7+
- **Database:** Postgres / DuckDB (depending on the profile)
- **Dependency Management:** `uv` (Astral)
- **Infrastructure:** Docker & Docker Compose

## 🚀 Quick Start

### 1. Environment Configuration (.env)

Create a `.env` file in the root of the project and copy the configuration below. These variables are used in `docker-compose.yaml` to initialize the Airflow admin and configure the database.

**`.env` example:**

```ini
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=your_password
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com

SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_wh
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_SCHEMA=your_schema
```

### 2\. Build & Run

Use **Makefile** or `docker-compose` to build and start all services.

#### 🔨 Build & Start (recommended for first run or after dependency updates)

```bash
make rebuild
# or manually:
docker-compose up -d --build --force-recreate
```

#### Start (existing containers)

```bash
make up
# or manually:
docker-compose up -d
```

### 3\. Access

  - **Airflow UI:** [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
  - **Login/Password:** As defined in your `.env` (default: `admin`/`admin`).

### 4\. Project Structure
```text
.
├── airflow/                         # Airflow-specific project area
│   ├── dags/
│   │   ├── retail_pipeline.py       # Main Airflow DAG using Cosmos
│   │   └── utils/                   # Utility modules (logging, callbacks, helpers)
│   ├── logs/                        # Airflow logs (mounted volume)
│   ├── plugins/                     # (Optional) custom Airflow plugins
│   └── README.md                    # Docs for Airflow DAGs / plugins
│
├── dbt_project/                     # dbt project root
│   ├── models/                      # dbt models (raw, staging, vault, marts)
│   ├── macros/                      # dbt macros
│   ├── seeds/                       # Input CSVs (e.g., raw_orders.csv)
│   ├── snapshots/                   # dbt snapshots
│   ├── profiles.yml                 # dbt profile for Snowflake/DuckDB
│   ├── dbt_project.yml              # dbt project config
│   └── README.md                    # Documentation for dbt project
│
├── docker-compose.yaml              # Multi-service orchestration (Airflow + Postgres)
├── Dockerfile                       # Airflow image with dbt & cosmos dependencies
├── Makefile                         # Shortcuts for build/run commands
├── pyproject.toml                   # Python project definition (uv/PEP 621)
├── requirements.txt                 # Exported dependencies for Airflow build
├── uv.lock                          # uv lockfile
├── scripts/                         # Utility scripts (optional)
└── README.md                        # You are here
```

### 5\. Development & Dependency Workflow

This project uses `uv` for dependency management.

**Adding a new Python library:**

1.  Add the dependency into `pyproject.toml`:

    ```toml
    dependencies = [
        "new-library>=1.0",
        ...
    ]
    ```

2.  Export dependencies for Docker:

    ```bash
    uv lock
    uv export --format requirements.txt --no-dev > requirements.txt
    ```

3.  Rebuild your environment:

    ```bash
    make rebuild
    ```

This ensures Airflow, dbt, Cosmos, and your DAGs all use a consistent dependency set.
