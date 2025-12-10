# dbt Retail Data Vault

Data transformation project implementing the Data Vault 2.0 architecture.

## 🏗 Layer Architecture

### 1. Staging (`models/staging`)
Initial cleaning of "raw" data.
- **`stg_orders.sql`**: Data preparation from CSV source (Seeds). Hashing Business Keys to generate Data Vault surrogate keys (Hash Keys).

### 2. Raw Vault (`models/raw_vault`)
Historical data storage layer.
- **Hubs (`hubs/`)**: Lists of unique business keys (e.g., `hub_customer`, `hub_order`).
- **Links (`links/`)**: Relationships between hubs (e.g., `link_customer_order`).
- **Satellites (`sats/`)**: Attributes and context changing over time (e.g., `sat_order_details`).

### 3. Business Marts (`models/marts`)
Data marts for analytics (Dimensional Model / Star Schema).
- **`fct_orders_history.sql`**: Fact table assembled by joining Hubs, Links, and Satellites.

## 💾 Data
- **Seeds**: Source data is loaded from `seeds/raw_orders.csv` using the `dbt seed` command.

## 🧪 Testing and Documentation
- **Tests**: Generic tests (unique, not_null) are defined in YAML files alongside models.
- **Docs**: Documentation generation for the lineage graph.

## ⚡️ Manual dbt Execution (from container)
If you want to run dbt commands outside of Airflow:

```bash
# Enter the container
docker-compose exec airflow-scheduler bash

# Navigate to project folder
cd $AIRFLOW_VAR_DBT_PROJECT_DIR

# Check connection
dbt debug

# Run models
dbt run
