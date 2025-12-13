FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y git && apt-get clean
USER airflow
WORKDIR /opt/airflow
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
RUN uv pip install -r pyproject.toml
COPY --chown=airflow:root dbt_vault_retail /opt/airflow/dbt_project
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root airflow/plugins /opt/airflow/plugins

ENV AIRFLOW_VAR_DBT_PROJECT_DIR=/opt/airflow/dbt_project
ENV AIRFLOW_VAR_DBT_PROFILES_DIR=/opt/airflow/dbt_project
