# dbt Retail Data Vault

This directory contains the **dbt transformation project** implementing a **Data Vault 2.0** architecture with downstream **Business Vault**, **PIT tables**, and **Dimensional Marts**.
The project is executed both locally (via Docker) and through **Apache Airflow (Cosmos)**.

---

## 🏗 Layered Architecture

The dbt project follows a strict, layered approach.
Each layer has a single responsibility and clear input/output contracts.

---

### 1️⃣ Staging Layer (`models/staging`)

**Purpose:**
Lightweight normalization of source data without business logic.

**Key characteristics:**
- One-to-one mapping with source tables
- Renaming columns
- Casting types
- No joins between sources
- No historical logic

**Examples:**
- `stg_orders.sql`
- `stg_customer.sql`
- `stg_lineitem.sql`

📌 These models act as the *entry point* into the Data Vault.

---

### 2️⃣ Raw Vault (`models/raw_vault`)

**Purpose:**
Store the full historical record of business events in a source-agnostic way.

#### 🔹 Hubs (`raw_vault/hubs`)
- Store **unique business keys**
- One row per business entity
- Non-temporal

Examples:
- `hub_customer`
- `hub_order`
- `hub_product`

#### 🔹 Links (`raw_vault/links`)
- Store **relationships between hubs**
- Non-temporal
- One row per unique relationship

Examples:
- `lnk_order_customer`
- `lnk_order_lineitem`

#### 🔹 Satellites (`raw_vault/sats`)
- Store **descriptive attributes**
- Track changes over time
- Use hashdiff-based change detection

Examples:
- `sat_customer_core`
- `sat_customer_contact`
- `sat_order_lineitem_measures`

📌 Satellites separate **business effective dates** from **technical load timestamps**.

---

### 3️⃣ Business Vault (`models/business_vault`)

**Purpose:**
Apply business rules and enrich Raw Vault data without breaking history.

#### 🔹 Business Satellites
- Derived or enriched attributes
- Reference data joins (e.g. seed-based master data)

Example:
- `bv_customer_master_sat`

#### 🔹 Effectivity Satellites
- Explicit validity ranges for business states

Example:
- `eff_sat_order_status`

#### 🔹 PIT Tables (`business_vault/pit`)
- **Point-in-Time snapshots**
- Allow reconstruction of entity state at any business date
- Built using:
  - Satellite effective dates
  - Link relationships
  - Business timelines

Examples:
- `pit_customer`
- `pit_order`
- `pit_order_customer`

📌 PIT tables **do not store descriptive attributes directly** — they resolve keys *as-of* a given date.

---

### 4️⃣ Data Marts (`models/marts`)

**Purpose:**
Provide analytics-ready tables for BI and reporting.

#### 🔹 Dimensions (`marts/dim`)
Built from PIT tables and Business Vault satellites.

Examples:
- `dim_customer`
- `dim_product`
- `dim_date`

#### 🔹 Facts (`marts/fct`)
Built from:
- Raw Vault links
- Raw Vault measure satellites
- PIT tables
- Dimensions

Example:
- `fct_sales`

📌 Fact tables **never read directly from staging**.

---

## 💾 Data Sources

- **Seeds**
  - Reference / master data
  - Loaded via `dbt seed`
  - Example: `customer_master.csv`

- **Warehouse**
  - Snowflake is used as the primary warehouse
  - Credentials are provided via **Airflow Connections**

---

## 🧪 Testing & Quality

- Generic tests:
  - `unique`
  - `not_null`
  - relationship tests
- Tests are defined in `.yml` files alongside models

---

## 📚 Documentation

dbt documentation includes:
- Model descriptions
- Column-level metadata
- Full lineage graph

Generate locally:
```bash
dbt docs generate
dbt docs serve
