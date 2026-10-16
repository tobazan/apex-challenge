.PHONY: setup lint test run-bronze clean

ENV ?= dev
TENANT ?= all
START_DATE ?= 2025-01-01
END_DATE ?= 2025-06-30

setup:
	uv venv .venv --python 3.11
	uv pip install -e ".[dev]"

lint:
	uv run ruff check src/ tests/

test:
	uv run pytest -v

run-bronze:
	uv run saas-pipeline --env $(ENV) --tenant $(TENANT) --start-date $(START_DATE) --end-date $(END_DATE) --layer bronze

clean:
	rm -rf .venv .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +