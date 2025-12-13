DC = docker compose
PRE_COMMIT = pre-commit

DBT_CONTAINER = airflow-webserver
RUNNER = python /opt/airflow/dags/utils/dbt_runner.py

.PHONY: help up down restart build logs lint clean

up: ## start containers
	$(DC) up -d

down: ## stop containers
	$(DC) down

build:
	$(DC) build --no-cache

restart: down up ## full restart

rebuild: down build up ## full rebuild and restart

logs: ## watch logs of airflow in realtime
	$(DC) logs -f webserver

lint: ## start all linters
	$(PRE_COMMIT) run --all-files

initial-load: ## Run Full Refresh (Seeds + Build from scratch)
	$(DC) exec $(DBT_CONTAINER) $(RUNNER) deps
	$(DC) exec $(DBT_CONTAINER) $(RUNNER) seed --full-refresh
	$(DC) exec $(DBT_CONTAINER) $(RUNNER) build --full-refresh

incremental-load: ## Run Incremental Load (Only new data)
	$(DC) exec $(DBT_CONTAINER) $(RUNNER) deps
	$(DC) exec $(DBT_CONTAINER) $(RUNNER) build

clean-up: ## Clean artifacts (local & docker)
	rm -rf dbt_vault_retail/target dbt_vault_retail/dbt_packages dbt_vault_retail/logs
	find . -type d -name "__pycache__" -exec rm -rf {} +
	-$(DC) exec $(DBT_CONTAINER) bash -c "cd /opt/airflow/dbt_project && dbt clean"

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
