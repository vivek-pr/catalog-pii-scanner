SHELL := /bin/bash

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
BLACK := $(VENV)/bin/black
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
PYTEST := $(VENV)/bin/pytest
PRECOMMIT := $(VENV)/bin/pre-commit
UVICORN := $(VENV)/bin/uvicorn

APP := catalog_pii_scanner.api:app
HOST ?= 127.0.0.1
PORT ?= 8000

.PHONY: setup fmt lint test check precommit clean

$(PY):
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip
	$(PY) -m pip install -e ".[dev]"
	$(PRECOMMIT) install

setup: $(PY)
	@echo "Environment ready."

fmt: $(PY)
	$(BLACK) src tests
	$(RUFF) check --fix src tests

lint: $(PY)
	$(RUFF) check src tests
	$(MYPY) src tests

test: $(PY)
	$(PYTEST)

check: fmt lint test

precommit: $(PY)
	$(PRECOMMIT) run --all-files

run-api: $(PY)
	$(UVICORN) $(APP) --host $(HOST) --port $(PORT) --reload

clean:
	rm -rf $(VENV) *.egg-info build dist .mypy_cache .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
