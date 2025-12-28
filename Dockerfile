FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git bash \
      postgresql postgresql-client postgresql-common \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && PG_BIN="$(find /usr/lib/postgresql -type f -name initdb 2>/dev/null | sort -V | tail -n 1 | xargs -r dirname)" \
 && ln -sf "${PG_BIN}/initdb" /usr/local/bin/initdb \
 && ln -sf "${PG_BIN}/pg_ctl" /usr/local/bin/pg_ctl \
 && ln -sf "${PG_BIN}/psql"   /usr/local/bin/psql
RUN id -u postgres >/dev/null 2>&1 || useradd -m -U -s /bin/bash postgres \
 && mkdir -p /var/lib/postgresql/data \
 && chown -R postgres:postgres /var/lib/postgresql
USER airflow
WORKDIR /opt/airflow
RUN pip install --no-cache-dir uv psycopg2-binary
COPY pyproject.toml uv.lock ./
RUN uv pip install -r pyproject.toml
COPY --chown=airflow:root dbt_vault_retail /opt/airflow/dbt_project
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root airflow/plugins /opt/airflow/plugins

ENV AIRFLOW_VAR_DBT_PROJECT_DIR=/opt/airflow/dbt_project
ENV AIRFLOW_VAR_DBT_PROFILES_DIR=/opt/airflow/dbt_project

COPY --chown=airflow:root docker/entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

USER root
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
CMD ["all"]
