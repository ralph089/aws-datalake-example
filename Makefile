.PHONY: help setup setup-lambda test test-integration test-lambda test-all lint lint-lambda format format-lambda type-check type-check-lambda run-local clean release-glue-dry-run release-lambda-dry-run release-glue release-lambda

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install glue-jobs dependencies and setup development environment
	@echo "Setting up glue-jobs development environment..."
	cd glue-jobs && uv sync --group dev
	@echo "✅ Glue-jobs setup completed!"

setup-lambda: ## Install lambda dependencies and setup development environment
	@echo "Setting up lambda development environment..."
	cd lambdas/data_lake_to_api && uv sync --group dev
	@echo "✅ Lambda setup completed!"

test: ## Run glue-jobs unit tests only
	@echo "Running glue-jobs unit tests..."
	cd glue-jobs && uv run pytest -m unit -v
	@echo "✅ Glue-jobs unit tests completed!"

test-lambda: ## Run lambda unit tests only
	@echo "Running lambda unit tests..."
	cd lambdas/data_lake_to_api && uv run pytest tests/ -v
	@echo "✅ Lambda unit tests completed!"

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

test-all: ## Run all tests (glue-jobs unit + integration + lambda unit)
	@echo "Running complete test suite..."
	@$(MAKE) test
	@$(MAKE) test-lambda
	@$(MAKE) test-integration
	@echo "✅ All tests completed!"

lint: ## Run glue-jobs linting checks
	@echo "Running glue-jobs linting checks..."
	cd glue-jobs && uv run ruff check .
	@echo "✅ Glue-jobs linting passed!"

lint-lambda: ## Run lambda linting checks
	@echo "Running lambda linting checks..."
	cd lambdas/data_lake_to_api && uv run ruff check src/ tests/
	@echo "✅ Lambda linting passed!"

format: ## Format glue-jobs code with ruff
	@echo "Formatting glue-jobs code..."
	cd glue-jobs && uv run ruff format .
	@echo "✅ Glue-jobs code formatted!"

format-lambda: ## Format lambda code with ruff
	@echo "Formatting lambda code..."
	cd lambdas/data_lake_to_api && uv run ruff format src/ tests/
	@echo "✅ Lambda code formatted!"

type-check: ## Run glue-jobs type checking with ty
	@echo "Running glue-jobs type checks..."
	cd glue-jobs && uv run ty src/
	@echo "✅ Glue-jobs type checking passed!"

type-check-lambda: ## Run lambda type checking with mypy
	@echo "Running lambda type checks..."
	cd lambdas/data_lake_to_api && uv run mypy src/
	@echo "✅ Lambda type checking passed!"

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
	rm -rf lambdas/data_lake_to_api/.coverage
	rm -rf lambdas/data_lake_to_api/.pytest_cache
	rm -rf lambdas/data_lake_to_api/package
	rm -f glue-jobs/requirements.txt
	rm -f lambdas/data_lake_to_api/requirements.txt
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

release-glue-dry-run: ## Test what the next glue-jobs version would be (dry run)
	@echo "🧪 Running glue-jobs semantic-release dry run..."
	cd glue-jobs && uv run semantic-release version --print
	@echo "✅ Glue-jobs dry run completed!"

release-lambda-dry-run: ## Test what the next lambda version would be (dry run)
	@echo "🧪 Running lambda semantic-release dry run..."
	cd lambdas/data_lake_to_api && uv run semantic-release version --print
	@echo "✅ Lambda dry run completed!"

release-glue: ## Force a glue-jobs release (usually handled by CI)
	@echo "🚀 Running glue-jobs semantic-release..."
	@echo "⚠️  Note: This is usually handled automatically by CI on main branch"
	cd glue-jobs && uv run semantic-release publish
	@echo "✅ Glue-jobs release completed!"

release-lambda: ## Force a lambda release (usually handled by CI)
	@echo "🚀 Running lambda semantic-release..."
	@echo "⚠️  Note: This is usually handled automatically by CI on main branch"
	cd lambdas/data_lake_to_api && uv run semantic-release publish
	@echo "✅ Lambda release completed!"

.DEFAULT_GOAL := help