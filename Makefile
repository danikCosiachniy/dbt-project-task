IMAGE = retail-airflow:latest
CONTAINER = retail-airflow

ENV_FILE = .env
PORT = 8080

PG_VOLUME = retail_airflow_pg
LOGS_DIR = ./airflow/logs

PRE_COMMIT = pre-commit
DOCKER = docker
EXEC = $(DOCKER) exec -i
# load .env into Make variables
include .env
export

# dbt runner inside container
RUNNER = python /opt/airflow/dags/utils/dbt_runner.py

# Airflow CLI inside running container
AIRFLOW = $(DOCKER) exec -u airflow -i $(CONTAINER) bash -lc

# DAG IDs
INITIAL_DAG = retail_vault_initial_dag
INCREMENTAL_DAG = retail_vault_incremental_dag
CLEAN_DAG = cleanup_database

.PHONY: help build up down restart rebuild logs lint ps initial-load incremental-load clean-up initial-load-runner incremental-load-runner clean-up-local

build: ## Build image
	$(DOCKER) build --no-cache -t $(IMAGE) .

up: ## Run container (Airflow+Postgres inside, entrypoint handles init)
	$(DOCKER) rm -f $(CONTAINER) 2>/dev/null || true
	$(DOCKER) run -d --name $(CONTAINER) \
	  --env-file $(ENV_FILE) \
	  -p $(PORT):8080 \
	  -v $(PG_VOLUME):/var/lib/postgresql/data \
	  -v "$(LOGS_DIR)":/opt/airflow/logs \
	  $(IMAGE)

down: ## Stop & remove container
	$(DOCKER) rm -f $(CONTAINER) 2>/dev/null || true

restart: down up ## Restart container

rebuild: ## Rebuild image and restart container
	make build
	make up

logs: ## Follow container logs
	$(DOCKER) logs -f $(CONTAINER)

ps: ## Show container status
	$(DOCKER) ps --filter "name=$(CONTAINER)"

lint: ## Run linters
	$(PRE_COMMIT) run --all-files

initial-load-runner: ## Run Full Refresh (Deps + Seeds + Build from scratch)
	$(EXEC) $(CONTAINER) $(RUNNER) deps
	$(EXEC) $(CONTAINER) $(RUNNER) seed --full-refresh
	$(EXEC) $(CONTAINER) $(RUNNER) build --full-refresh

incremental-load-runner: ## Run Incremental Load (Only new data)
	$(EXEC) $(CONTAINER) $(RUNNER) deps
	$(EXEC) $(CONTAINER) $(RUNNER) build

clean-up-local: ## Clean artifacts (local + dbt clean inside container)
	rm -rf dbt_vault_retail/target dbt_vault_retail/dbt_packages dbt_vault_retail/logs
	find . -type d -name "__pycache__" -exec rm -rf {} +
	-$(EXEC) $(CONTAINER) bash -lc "cd /opt/airflow/dbt_project && dbt clean"

initial-load: ## Trigger FULL load DAG (full_refresh=True)
	$(AIRFLOW) "airflow dags trigger $(INITIAL_DAG)"

incremental-load: ## Trigger INCREMENTAL load DAG (full_refresh=False)
	$(AIRFLOW) "airflow dags trigger $(INCREMENTAL_DAG)"

clean-up: ## Trigger cleanup DAG (drops schemas by prefix)
	$(AIRFLOW) "airflow dags trigger $(CLEAN_DAG)"

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
