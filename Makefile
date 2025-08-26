.PHONY: help setup test test-integration test-all lint format type-check run-local clean

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies and setup development environment
	@echo "Setting up development environment..."
	cd glue-jobs && uv sync
	cd glue-jobs && uv sync --group dev
	@echo "✅ Setup completed!"

test: ## Run unit tests only
	@echo "Running unit tests..."
	cd glue-jobs && uv run pytest -m unit -v
	@echo "✅ Unit tests completed!"

test-integration: ## Run integration tests in Docker
	@echo "🐳 Starting Docker integration tests..."
	@echo "📦 Using Docker-optimized requirements.txt (PySpark pre-installed in container)..."
	@$(MAKE) requirements-docker
	cd glue-jobs && docker-compose up -d
	@echo "⏳ Waiting for container to be ready..."
	@sleep 15
	cd glue-jobs && docker-compose exec -T glue bash -c "pip install --user -r requirements.txt && python -m pytest -m integration --with-integration -v --tb=short"
	@echo "🛑 Stopping Docker containers..."
	cd glue-jobs && docker-compose down
	@echo "✅ Integration tests completed!"

test-all: ## Run all tests (unit + integration)
	@echo "Running complete test suite..."
	@$(MAKE) test
	@$(MAKE) test-integration
	@echo "✅ All tests completed!"

lint: ## Run linting checks
	@echo "Running linting checks..."
	cd glue-jobs && uv run ruff check .
	@echo "✅ Linting passed!"

format: ## Format code with ruff
	@echo "Formatting code..."
	cd glue-jobs && uv run ruff format .
	@echo "✅ Code formatted!"

type-check: ## Run type checking with mypy
	@echo "Running type checks..."
	cd glue-jobs && uv run mypy src/
	@echo "✅ Type checking passed!"

run-local: ## Run job locally (usage: make run-local JOB=simple_etl)
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Error: Please specify JOB parameter"; \
		echo "Usage: make run-local JOB=simple_etl"; \
		echo "       make run-local JOB=api_to_lake"; \
		exit 1; \
	fi
	@echo "🔧 Running $(JOB) locally..."
	cd glue-jobs && PYTHONPATH=src uv run python src/jobs/$(JOB).py
	@echo "✅ Job $(JOB) completed!"

clean: ## Clean up generated files
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dist/
	rm -rf glue-jobs/.coverage
	rm -rf glue-jobs/.pytest_cache
	rm -f glue-jobs/requirements.txt
	@echo "✅ Cleanup completed!"

requirements-docker: ## Generate Docker-optimized requirements.txt (excludes pre-installed packages)
	@echo "📦 Generating Docker-optimized requirements.txt..."
	@cd glue-jobs && echo "# Docker requirements for AWS Glue integration tests" > requirements.txt
	@cd glue-jobs && echo "# PySpark, PyArrow, boto3, pandas, numpy are pre-installed in the Glue container" >> requirements.txt
	@cd glue-jobs && echo "" >> requirements.txt
	@cd glue-jobs && uv export --no-hashes --no-emit-project | \
		grep -v "^#" | \
		grep -v "^$$" | \
		grep -v "^pyspark==" | \
		grep -v "^boto3==" | \
		grep -v "^pyarrow==" | \
		grep -v "^pandas==" | \
		grep -v "^numpy==" | \
		grep -v "^botocore==" | \
		grep -v "^s3transfer==" >> requirements.txt
	@echo "✅ Docker-optimized requirements.txt generated!"

.DEFAULT_GOAL := help