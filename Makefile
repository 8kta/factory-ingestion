.PHONY: help install install-dev test test-cov test-verbose clean lint format

help:
	@echo "Available commands:"
	@echo "  make install       - Install production dependencies"
	@echo "  make install-dev   - Install development dependencies"
	@echo "  make test          - Run all tests"
	@echo "  make test-cov      - Run tests with coverage report"
	@echo "  make test-verbose  - Run tests with verbose output"
	@echo "  make clean         - Remove generated files"
	@echo "  make lint          - Run code linting"
	@echo "  make format        - Format code with black"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-test.txt

test:
	pytest

test-cov:
	pytest --cov=src --cov-report=term-missing --cov-report=html

test-verbose:
	pytest -v

clean:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

lint:
	flake8 src tests

format:
	black src tests
