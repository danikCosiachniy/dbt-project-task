#!/usr/bin/env bash
set -euo pipefail

INITDB=/usr/local/bin/initdb
PG_CTL=/usr/local/bin/pg_ctl
PSQL=/usr/local/bin/psql

: "${POSTGRES_USER}"
: "${POSTGRES_PASSWORD}"
: "${POSTGRES_DB}"
: "${PGDATA}"

: "${AIRFLOW__CORE__EXECUTOR:=LocalExecutor}"

export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB PGDATA
export AIRFLOW__CORE__EXECUTOR PGDATA

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
unset AIRFLOW__CORE__SQL_ALCHEMY_CONN || true

run_as_postgres() { su -s /bin/bash postgres -c "$*"; }
run_as_airflow()  { su -s /bin/bash airflow  -c "$*"; }

ensure_pgdata_perms() {
  mkdir -p "${PGDATA}"
  chown -R postgres:postgres "${PGDATA}"
  chmod 700 "${PGDATA}" || true
}

require_bins() {
  for b in "${INITDB}" "${PG_CTL}" "${PSQL}"; do
    if [[ ! -x "$b" ]]; then
      echo "[entrypoint] ERROR: required postgres binary not found/executable: $b"
      exit 1
    fi
  done
}

init_postgres_if_needed() {
  if [[ ! -s "${PGDATA}/PG_VERSION" ]]; then
    echo "[entrypoint] initdb..."
    ensure_pgdata_perms
    run_as_postgres "${INITDB} -D '${PGDATA}'"
  fi
}

start_postgres() {
  echo "[entrypoint] starting postgres..."
  ensure_pgdata_perms
  run_as_postgres "${PG_CTL} -D '${PGDATA}' -o \"-c listen_addresses='127.0.0.1' -p 5432\" -w start"
}

stop_postgres() {
  echo "[entrypoint] stopping postgres..."
  run_as_postgres "${PG_CTL} -D '${PGDATA}' -m fast -w stop" || true
}

ensure_role_and_db() {
  echo "[entrypoint] ensure role '${POSTGRES_USER}'..."
  run_as_postgres "${PSQL} -p 5432 -v ON_ERROR_STOP=1 -d postgres -c \"DO \\$\\$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='${POSTGRES_USER}') THEN
      CREATE ROLE ${POSTGRES_USER} WITH LOGIN PASSWORD '${POSTGRES_PASSWORD}';
    END IF;
  END \\$\\$;\""

  echo "[entrypoint] ensure database '${POSTGRES_DB}'..."
  run_as_postgres "DB_EXISTS=\$(${PSQL} -p 5432 -tAc \"SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'\" -d postgres | tr -d '[:space:]'); \
    if [[ \"\${DB_EXISTS}\" != \"1\" ]]; then \
      ${PSQL} -p 5432 -v ON_ERROR_STOP=1 -d postgres -c \"CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_USER};\"; \
    else \
      echo \"[entrypoint] database '${POSTGRES_DB}' already exists\"; \
    fi"
}

SCHED_PID=""
trap '[[ -n "${SCHED_PID}" ]] && kill "${SCHED_PID}" 2>/dev/null || true; stop_postgres' TERM INT EXIT

require_bins
init_postgres_if_needed
start_postgres
ensure_role_and_db

echo "[entrypoint] airflow db migrate (DB=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN})..."
run_as_airflow "airflow db migrate"

if [[ -n "${AIRFLOW_ADMIN_USER:-}" && -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
  echo "[entrypoint] ensure admin user..."
  run_as_airflow "airflow users create \
    --username \"${AIRFLOW_ADMIN_USER}\" \
    --password \"${AIRFLOW_ADMIN_PASSWORD}\" \
    --firstname \"${AIRFLOW_ADMIN_FIRSTNAME:-Admin}\" \
    --lastname \"${AIRFLOW_ADMIN_LASTNAME:-User}\" \
    --role Admin \
    --email \"${AIRFLOW_ADMIN_EMAIL:-admin@example.com}\"" || true
else
  echo "[entrypoint] AIRFLOW_ADMIN_USER/PASSWORD not set -> skip user create"
fi

mode="${1:-all}"
case "$mode" in
  all)
    echo "[entrypoint] starting scheduler (bg) + webserver (fg)"
    su -s /bin/bash airflow -c "airflow scheduler" &
    SCHED_PID="$!"
    exec su -s /bin/bash airflow -c "airflow webserver"
    ;;
  webserver) exec su -s /bin/bash airflow -c "airflow webserver" ;;
  scheduler) exec su -s /bin/bash airflow -c "airflow scheduler" ;;
  *) exec "$@" ;;
esac
