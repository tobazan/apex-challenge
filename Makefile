.PHONY: setup lint fix-lint test run-bronze clean-delta clean-env clean-all

ENV ?= dev
TENANT ?= all
START_DATE ?= 2025-01-01
END_DATE ?= 2025-06-30

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

# Parameterized bronze ingestion
run-bronze:
	uv run saas_pipeline --env $(ENV) --tenant $(TENANT) --start-date $(START_DATE) --end-date $(END_DATE) --layer bronze

# Clean Delta storage outputs, checkpoints and local Spark metastores
clean-delta:
	rm -rf data/bronze data/silver data/gold data/shared data/_checkpoints
	rm -rf data/qa data/main
	rm -rf spark-warehouse metastore_db derby.log .spark-staging

# Clean virtual environment and test/lint caches
clean-env:
	rm -rf .venv .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +