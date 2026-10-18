.PHONY: setup test test-e2e lint fix-lint run-bronze run-silver run-gold run-all clean-delta clean-delta-all clean-env 


ENV ?= dev
RUNTIME ?= filesystem
TENANT ?= all
FAIL_FAST ?=
FAIL_CRITICAL ?=

EXTRA_FLAGS := $(FAIL_FAST) $(FAIL_CRITICAL)


# Environment setup with uv
setup:
	uv venv .venv --python 3.11
	uv pip install -e ".[dev]"

# Linting and auto-formatting
lint:
	uv run ruff check src/ tests/

fix-lint:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

# Test suite
test:
	uv run pytest -v

# Local full-dataset E2E test
test-e2e:
	uv run pytest -v -m e2e

# Parameterized pipeline running
run-bronze:
	uv run saas_pipeline --env $(ENV) --runtime $(RUNTIME) --tenant $(TENANT) --layer bronze $(EXTRA_FLAGS)

run-silver:
	uv run saas_pipeline --env $(ENV) --runtime $(RUNTIME) --tenant $(TENANT) --layer silver $(EXTRA_FLAGS)

run-gold:
	uv run saas_pipeline --env $(ENV) --runtime $(RUNTIME) --tenant $(TENANT) --layer gold $(EXTRA_FLAGS)

run-all:
	uv run saas_pipeline --env $(ENV) --runtime $(RUNTIME) --tenant $(TENANT) --layer all $(EXTRA_FLAGS)

# Clean Delta storage outputs, checkpoints and local Spark metastores
clean-delta:
	@echo "Cleaning local Delta tables and checkpoints for ENV=$(ENV)..."
	rm -rf data/$(ENV)
	rm -rf spark-warehouse metastore_db derby.log

clean-delta-all:
	@echo "Cleaning local Delta tables across all environments (dev, qa, main)..."
	rm -rf data/dev data/qa data/main
	rm -rf spark-warehouse metastore_db derby.log

# Clean virtual environment and test/lint caches
clean-env:
	rm -rf .venv .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +