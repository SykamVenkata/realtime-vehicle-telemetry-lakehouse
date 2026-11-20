.PHONY: help up down logs show demo-batch test test-fast lint fmt clean

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

up:  ## Start Kafka and the full streaming pipeline in Docker
	docker compose up --build -d
	@echo "Pipeline is starting. Give it ~2 minutes, then run: make show"

down:  ## Stop everything (keeps ./data)
	docker compose down

logs:  ## Follow the pipeline logs
	docker compose exec app tail -f /app/data/logs/generator.log /app/data/logs/bronze.log /app/data/logs/silver.log /app/data/logs/gold.log

show:  ## Row counts, quarantine reasons, latest metrics, alerts and latency
	docker compose exec app python scripts/show_gold.py

demo-batch:  ## Build the daily summary and run table maintenance
	docker compose exec app python pipelines/gold_daily_summary.py
	docker compose exec app python pipelines/maintenance.py

test:  ## All tests, including Spark (needs: pip install -e '.[spark,dev]' and Java 17)
	pytest -q

test-fast:  ## Pure-Python tests only (no Spark needed)
	pytest -q -m "not spark"

lint:  ## Ruff lint
	ruff check .

fmt:  ## Ruff format
	ruff format .

clean:  ## Delete local Lakehouse data and checkpoints
	rm -rf data/
