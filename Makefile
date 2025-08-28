.PHONY: help setup setup-lambda test test-integration test-lambda test-all lint lint-lambda format format-lambda type-check type-check-lambda run-local clean clean-lambda package-jobs package-lambda list-lambdas release-glue-dry-run release-lambda-dry-run release-glue release-lambda

# Lambda discovery
LAMBDA_DIRS := $(shell find lambdas -maxdepth 1 -type d -name "*" ! -name "lambdas" | sort)
LAMBDAS := $(notdir $(LAMBDA_DIRS))

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Available lambdas: $(LAMBDAS)"

setup: ## Install glue-jobs dependencies and setup development environment
	@echo "Setting up glue-jobs development environment..."
	cd glue-jobs && uv sync --group dev
	@echo "✅ Glue-jobs setup completed!"

setup-lambda: ## Install lambda dependencies (usage: make setup-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Setting up lambda: $$lambda"; \
				cd lambdas/$$lambda && uv sync --group dev && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Setting up lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv sync --group dev; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Setting up all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Setting up lambda: $$lambda"; \
			cd lambdas/$$lambda && uv sync --group dev && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda setup completed!"

test: ## Run glue-jobs unit tests only
	@echo "Running glue-jobs unit tests..."
	cd glue-jobs && uv run pytest -m unit -v
	@echo "✅ Glue-jobs unit tests completed!"

test-lambda: ## Run lambda unit tests (usage: make test-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Running tests for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run pytest tests/ -v && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Running tests for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run pytest tests/ -v; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Running tests for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Running tests for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run pytest tests/ -v && cd ../..; \
		done; \
	fi
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

lint-lambda: ## Run lambda linting checks (usage: make lint-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Running linting for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run ruff check src/ tests/ && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Running linting for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run ruff check src/ tests/; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Running linting for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Running linting for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run ruff check src/ tests/ && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda linting passed!"

lint-lambda-fix: ## Run lambda linting checks and fix (usage: make lint-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Running linting for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run ruff check --fix src/ tests/ && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Running linting for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run ruff check --fix  src/ tests/; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Running linting for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Running linting for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run ruff check --fix src/ tests/ && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda linting passed!"

format: ## Format glue-jobs code with ruff
	@echo "Formatting glue-jobs code..."
	cd glue-jobs && uv run ruff format .
	@echo "✅ Glue-jobs code formatted!"

format-lambda: ## Format lambda code with ruff (usage: make format-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Formatting lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run ruff format src/ tests/ && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Formatting lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run ruff format src/ tests/; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Formatting all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Formatting lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run ruff format src/ tests/ && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda code formatted!"

type-check: ## Run glue-jobs type checking with ty
	@echo "Running glue-jobs type checks..."
	cd glue-jobs && uv run ty src/
	@echo "✅ Glue-jobs type checking passed!"

type-check-lambda: ## Run lambda type checking with mypy (usage: make type-check-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "Running type check for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run mypy src/ && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Running type check for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run mypy src/; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Running type checks for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Running type check for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run mypy src/ && cd ../..; \
		done; \
	fi
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

package-jobs: ## Build glue-jobs wheel package for deployment
	@echo "📦 Building glue-jobs wheel package..."
	@mkdir -p dist
	cd glue-jobs && uv sync
	cd glue-jobs && uv build
	@cp glue-jobs/dist/*.whl dist/
	@echo "📋 Exporting requirements for external dependencies..."
	cd glue-jobs && uv export --format requirements-txt --no-hashes --no-emit-project > ../dist/requirements.txt
	@echo "✅ Glue-jobs package created in dist/"
	@ls -la dist/

clean: ## Clean up generated files
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dist/
	rm -rf glue-jobs/dist/
	rm -rf glue-jobs/.coverage
	rm -rf glue-jobs/.pytest_cache
	rm -f glue-jobs/requirements.txt
	@for lambda in $(LAMBDAS); do \
		echo "Cleaning lambda: $$lambda"; \
		rm -rf lambdas/$$lambda/.coverage; \
		rm -rf lambdas/$$lambda/.pytest_cache; \
		rm -rf lambdas/$$lambda/package; \
		rm -f lambdas/$$lambda/requirements.txt; \
	done
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

release-lambda-dry-run: ## Test what the next lambda version would be (usage: make release-lambda-dry-run LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "🧪 Running semantic-release dry run for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run semantic-release version --print && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "🧪 Running semantic-release dry run for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run semantic-release version --print; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "🧪 Running semantic-release dry run for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "🧪 Running semantic-release dry run for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run semantic-release version --print && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda dry run completed!"

release-glue: ## Force a glue-jobs release (usually handled by CI)
	@echo "🚀 Running glue-jobs semantic-release..."
	@echo "⚠️  Note: This is usually handled automatically by CI on main branch"
	cd glue-jobs && uv run semantic-release publish
	@echo "✅ Glue-jobs release completed!"

release-lambda: ## Force a lambda release (usage: make release-lambda LAMBDA=sns_glue_trigger or all)
	@echo "⚠️  Note: This is usually handled automatically by CI on main branch"
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			for lambda in $(LAMBDAS); do \
				echo "🚀 Running semantic-release for lambda: $$lambda"; \
				cd lambdas/$$lambda && uv run semantic-release publish && cd ../..; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "🚀 Running semantic-release for lambda: $(LAMBDA)"; \
				cd lambdas/$(LAMBDA) && uv run semantic-release publish; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "🚀 Running semantic-release for all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "🚀 Running semantic-release for lambda: $$lambda"; \
			cd lambdas/$$lambda && uv run semantic-release publish && cd ../..; \
		done; \
	fi
	@echo "✅ Lambda release completed!"

list-lambdas: ## List all available lambdas
	@echo "Available lambdas:"
	@for lambda in $(LAMBDAS); do \
		echo "  - $$lambda"; \
	done

package-lambda: ## Package specific lambda for deployment (usage: make package-lambda LAMBDA=sns_glue_trigger)
	@if [ -z "$(LAMBDA)" ]; then \
		echo "❌ Error: Please specify LAMBDA parameter"; \
		echo "Usage: make package-lambda LAMBDA=sns_glue_trigger"; \
		echo "Available lambdas: $(LAMBDAS)"; \
		exit 1; \
	fi
	@if [ ! -d "lambdas/$(LAMBDA)" ]; then \
		echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
		echo "Available lambdas: $(LAMBDAS)"; \
		exit 1; \
	fi
	@echo "📦 Packaging lambda: $(LAMBDA)"
	@cd lambdas/$(LAMBDA) && \
		if [ -f "scripts/package.sh" ]; then \
			./scripts/package.sh; \
		else \
			echo "⚠️  No package.sh script found for $(LAMBDA)"; \
			echo "Creating basic package..."; \
			mkdir -p package; \
			cp -r src/* package/; \
			echo "✅ Basic package created in lambdas/$(LAMBDA)/package/"; \
		fi
	@echo "✅ Lambda $(LAMBDA) packaged!"

clean-lambda: ## Clean specific lambda or all lambdas (usage: make clean-lambda LAMBDA=sns_glue_trigger or all)
	@if [ -n "$(LAMBDA)" ]; then \
		if [ "$(LAMBDA)" = "all" ]; then \
			echo "Cleaning all lambdas..."; \
			for lambda in $(LAMBDAS); do \
				echo "Cleaning lambda: $$lambda"; \
				rm -rf lambdas/$$lambda/.coverage; \
				rm -rf lambdas/$$lambda/.pytest_cache; \
				rm -rf lambdas/$$lambda/package; \
				rm -f lambdas/$$lambda/requirements.txt; \
				find lambdas/$$lambda -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true; \
				find lambdas/$$lambda -type f -name "*.pyc" -delete 2>/dev/null || true; \
			done; \
		else \
			if [ -d "lambdas/$(LAMBDA)" ]; then \
				echo "Cleaning lambda: $(LAMBDA)"; \
				rm -rf lambdas/$(LAMBDA)/.coverage; \
				rm -rf lambdas/$(LAMBDA)/.pytest_cache; \
				rm -rf lambdas/$(LAMBDA)/package; \
				rm -f lambdas/$(LAMBDA)/requirements.txt; \
				find lambdas/$(LAMBDA) -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true; \
				find lambdas/$(LAMBDA) -type f -name "*.pyc" -delete 2>/dev/null || true; \
			else \
				echo "❌ Error: Lambda '$(LAMBDA)' not found"; \
				echo "Available lambdas: $(LAMBDAS)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "Cleaning all lambdas..."; \
		for lambda in $(LAMBDAS); do \
			echo "Cleaning lambda: $$lambda"; \
			rm -rf lambdas/$$lambda/.coverage; \
			rm -rf lambdas/$$lambda/.pytest_cache; \
			rm -rf lambdas/$$lambda/package; \
			rm -f lambdas/$$lambda/requirements.txt; \
			find lambdas/$$lambda -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true; \
			find lambdas/$$lambda -type f -name "*.pyc" -delete 2>/dev/null || true; \
		done; \
	fi
	@echo "✅ Lambda cleanup completed!"

.DEFAULT_GOAL := help