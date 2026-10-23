SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv
PY := $(VENV)/bin/python
COMPOSE ?= docker compose

COUNT ?= 6000
RATE ?= 400
IDLE_TIMEOUT ?= 20
SEED ?= 42

ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help install up down clean ps logs topics init-db generate-events produce consume \
        dashboard analytics report dlq-inspect test test-integration lint format build demo

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

$(PY):
	$(PYTHON) -m venv $(VENV)

install: $(PY) ## Create .venv and install the package with dev, dashboard and report extras
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev,dashboard,report]"

up: .env ## Start Redpanda and ClickHouse, wait for health, create topics
	$(COMPOSE) up -d --wait redpanda clickhouse
	$(COMPOSE) run --rm topics-init

down: ## Stop containers (data volumes are kept)
	$(COMPOSE) --profile app down

clean: ## Stop containers and delete data volumes
	$(COMPOSE) --profile app down --volumes

ps: ## Show container status
	$(COMPOSE) --profile app ps -a

logs: ## Follow infrastructure logs
	$(COMPOSE) logs -f redpanda clickhouse

topics: ## Describe the Kafka topics
	$(COMPOSE) exec redpanda rpk topic describe $(STREAMPULSE_EVENTS_TOPIC) $(STREAMPULSE_DLQ_TOPIC)

init-db: ## Apply sql/schema.sql (the consumer also does this on start)
	$(PY) -m streampulse.storage.migrate

generate-events: ## Write deterministic sample events to data/sample_events.jsonl
	$(PY) -m streampulse.events.generator --count 1000 --seed $(SEED) --output data/sample_events.jsonl

produce: ## Publish COUNT synthetic events at RATE msg/s from the host
	$(PY) -m streampulse.producer.main --count $(COUNT) --rate $(RATE) --seed $(SEED)

consume: ## Consume into ClickHouse from the host; stops after IDLE_TIMEOUT idle seconds
	$(PY) -m streampulse.consumer.main --idle-timeout $(IDLE_TIMEOUT)

dashboard: ## Run the Streamlit dashboard on the host (http://localhost:8501)
	$(VENV)/bin/streamlit run dashboard/app.py --browser.gatherUsageStats=false

analytics: ## Run every query in sql/analytics.sql and print the results
	$(PY) scripts/run_analytics.py

report: ## Render docs/images/analytics_report.png from live query results
	$(PY) scripts/render_report.py --output docs/images/analytics_report.png

dlq-inspect: ## Summarise dead-lettered messages
	$(PY) scripts/dlq_tool.py inspect

test: ## Run unit tests (no infrastructure needed)
	$(PY) -m pytest tests/unit -q

test-integration: ## Run integration tests against the running stack
	STREAMPULSE_RUN_INTEGRATION=1 $(PY) -m pytest tests/integration -v

lint: ## Ruff lint and format check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format: ## Apply Ruff fixes and formatting
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

build: .env ## Build the application image
	$(COMPOSE) --profile app build

demo: up build ## Full containerised run: consumer and producer in parallel, then analytics
	-@docker rm -f streampulse-demo-consumer >/dev/null 2>&1
	$(COMPOSE) run -d --name streampulse-demo-consumer consumer --idle-timeout $(IDLE_TIMEOUT)
	$(COMPOSE) run --rm producer --count $(COUNT) --rate $(RATE) --seed $(SEED)
	docker wait streampulse-demo-consumer
	docker logs --tail 3 streampulse-demo-consumer
	docker rm streampulse-demo-consumer >/dev/null
	$(COMPOSE) run --rm --entrypoint python consumer scripts/run_analytics.py \
		pipeline_overview funnel_summary duplicate_and_invalid_rates late_events_by_type \
		rejections_by_error_type consumer_lag_latest
