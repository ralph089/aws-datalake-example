.PHONY: help setup test lint format run-local package deploy-infra deploy-jobs clean install-dev check-deps

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies and setup development environment
	@echo "Setting up development environment..."
	cd glue-jobs && uv sync
	pre-commit install
	@echo "✅ Setup completed!"

install-dev: ## Install development dependencies only
	cd glue-jobs && uv sync --dev


check-deps: ## Check for dependency updates
	cd glue-jobs && uv tree --outdated

test: test-unit test-int ## Run all tests

test-unit: ## Run unit tests with coverage
	@echo "Running unit tests..."
	cd glue-jobs && uv run pytest tests/unit -v --cov=src --cov-report=term-missing

test-int: ## Run integration tests in Docker
	@echo "Running integration tests..."
	@echo "Generating requirements.txt..."
	cd glue-jobs && uv export --format requirements-txt --no-hashes --no-editable --dev | grep -v "^\\." > requirements.txt
	cd glue-jobs && docker-compose up -d glue
	@echo "Waiting for container startup and dependency installation..."
	@echo "This may take several minutes for the first run..."
	@timeout=120; while [ $$timeout -gt 0 ]; do \
		if docker exec glue-local python -c "import moto; import pytest; import structlog; print('All dependencies ready')" 2>/dev/null; then \
			echo "✅ Dependencies installation completed"; \
			break; \
		fi; \
		echo "Waiting for dependencies... ($$timeout seconds remaining)"; \
		sleep 10; \
		timeout=$$((timeout - 10)); \
	done; \
	if [ $$timeout -le 0 ]; then \
		echo "❌ Timeout waiting for dependencies installation"; \
		exit 1; \
	fi
	@echo "Checking installation progress..."
	docker logs glue-local | tail -5
	@echo "Testing container connectivity and Python environment..."
	docker exec glue-local python -c "print('✅ Container is running and Python works')"
	docker exec glue-local python -c "import sys; print(f'Python version: {sys.version}')"
	docker exec glue-local python -c "import pyspark; print(f'✅ PySpark available: {pyspark.__version__}')"
	@echo "Testing installed dependencies..."
	docker exec glue-local python -c "import moto; print('✅ Moto available for testing')"
	docker exec glue-local python -c "import pytest; print('✅ Pytest available')"
	docker exec glue-local python -c "import structlog; print('✅ Structlog available')"
	@echo "Running full integration test suite..."
	docker exec glue-local bash -c "cd /home/hadoop/workspace && PYTHONPATH=/home/hadoop/workspace:/home/hadoop/workspace/src pytest tests/integration -v --tb=short --with-integration"
	@echo "✅ All integration tests completed!"
	cd glue-jobs && docker-compose down

test-watch: ## Run tests in watch mode
	cd glue-jobs && uv run pytest-watch tests/unit

lint: ## Run linting checks
	@echo "Running linting checks..."
	cd glue-jobs && uv run ruff check .
	@echo "✅ Linting passed!"

format: ## Format code with black and ruff
	@echo "Formatting code..."
	cd glue-jobs && uv run black .
	cd glue-jobs && uv run ruff format .
	@echo "✅ Code formatted!"

type-check: ## Run type checking with mypy
	cd glue-jobs && uv run mypy src/

pre-commit: ## Run all pre-commit hooks
	pre-commit run --all-files

validate-terraform: ## Validate Terraform configurations
	@echo "Validating Terraform configurations..."
	@for env in dev staging prod; do \
		echo "Validating $$env environment..."; \
		cd infrastructure/environments/$$env && \
		terraform init -backend=false && \
		terraform validate && \
		cd - > /dev/null; \
	done
	@echo "✅ Terraform validation passed!"

run-local: ## Run job locally (usage: make run-local JOB=customer_import, add VERBOSE=true for full output)
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Error: Please specify JOB parameter"; \
		echo "Usage: make run-local JOB=customer_import"; \
		echo "       make run-local JOB=customer_import VERBOSE=true  # for debugging"; \
		exit 1; \
	fi
	@if [ "$(VERBOSE)" = "true" ]; then \
		echo "Running $(JOB) locally in verbose mode..."; \
		echo "Generating requirements.txt..."; \
	else \
		echo "🔧 Preparing job: $(JOB)"; \
	fi
	@cd glue-jobs && uv export --format requirements-txt --no-hashes --no-editable --dev | sed '/^\.$$/d' > requirements.txt $(if $(filter-out true,$(VERBOSE)),2>/dev/null)
	@$(MAKE) package $(if $(filter-out true,$(VERBOSE)),>/dev/null 2>&1)
	@if [ "$(VERBOSE)" != "true" ]; then echo "📦 Package ready"; fi
	@VERBOSE=$(VERBOSE) ./glue-jobs/scripts/run_local.sh $(JOB)

run-all-local: ## Run all jobs locally for testing
	@echo "Running all jobs locally..."
	@$(MAKE) run-local JOB=customer_import
	@$(MAKE) run-local JOB=sales_etl
	@$(MAKE) run-local JOB=inventory_sync
	@$(MAKE) run-local JOB=product_catalog

package: ## Package jobs for deployment
	@if [ "$(VERBOSE)" = "true" ]; then echo "Packaging Glue jobs..."; fi
	@mkdir -p dist
	@cd glue-jobs && ./scripts/package.sh $(if $(filter-out true,$(VERBOSE)),>/dev/null 2>&1)
	@if [ "$(VERBOSE)" = "true" ]; then echo "✅ Package created in dist/utils.zip"; fi

deploy-infra: ## Deploy infrastructure (usage: make deploy-infra ENV=dev ACTION=plan)
	@if [ -z "$(ENV)" ]; then \
		echo "❌ Error: Please specify ENV parameter (dev/staging/prod)"; \
		exit 1; \
	fi
	@if [ -z "$(ACTION)" ]; then \
		echo "❌ Error: Please specify ACTION parameter (plan/apply)"; \
		exit 1; \
	fi
	@echo "Deploying infrastructure to $(ENV) with action $(ACTION)..."
	cd infrastructure/environments/$(ENV) && \
		terraform init && \
		terraform $(ACTION)

deploy-jobs: ## Deploy Glue job scripts (usage: make deploy-jobs ENV=dev)
	@if [ -z "$(ENV)" ]; then \
		echo "❌ Error: Please specify ENV parameter (dev/staging/prod)"; \
		exit 1; \
	fi
	@echo "Deploying Glue job scripts to $(ENV)..."
	@$(MAKE) package
	aws s3 cp dist/utils.zip s3://glue-scripts-$(ENV)/libs/main/
	aws s3 cp glue-jobs/src/jobs/ s3://glue-scripts-$(ENV)/jobs/main/ --recursive --exclude "*.pyc" --exclude "__pycache__"
	@echo "✅ Jobs deployed to $(ENV)!"

deploy-all: ## Deploy both infrastructure and jobs (usage: make deploy-all ENV=dev)
	@$(MAKE) deploy-infra ENV=$(ENV) ACTION=apply
	@$(MAKE) deploy-jobs ENV=$(ENV)

logs: ## View recent Glue job logs (usage: make logs JOB=customer_import ENV=dev)
	@if [ -z "$(JOB)" ] || [ -z "$(ENV)" ]; then \
		echo "❌ Error: Please specify JOB and ENV parameters"; \
		echo "Usage: make logs JOB=customer_import ENV=dev"; \
		exit 1; \
	fi
	aws logs tail /aws-glue/jobs/logs-v2 --follow --filter-pattern "$(JOB)-$(ENV)"

job-status: ## Check status of Glue jobs (usage: make job-status ENV=dev)
	@if [ -z "$(ENV)" ]; then \
		echo "❌ Error: Please specify ENV parameter"; \
		exit 1; \
	fi
	@echo "Checking job status for $(ENV) environment..."
	@for job in customer_import sales_etl inventory_sync product_catalog; do \
		echo "🔍 Checking $$job-$(ENV)..."; \
		aws glue get-job-runs --job-name $$job-$(ENV) --max-results 3 --query 'JobRuns[*].[JobRunState,StartedOn,ExecutionTime]' --output table || echo "   Job not found"; \
	done

clean: ## Clean up generated files and containers
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dist/
	rm -rf glue-jobs/dist/
	rm -rf glue-jobs/.coverage
	rm -rf glue-jobs/requirements.txt
	cd glue-jobs && docker-compose down --volumes --remove-orphans 2>/dev/null || true
	@echo "✅ Cleanup completed!"

docker-up: ## Start Docker containers
	cd glue-jobs && docker-compose up -d

docker-down: ## Stop Docker containers
	cd glue-jobs && docker-compose down

docker-logs: ## View Docker container logs
	cd glue-jobs && docker-compose logs -f glue

docker-shell: ## Open shell in Glue container
	docker exec -it glue-local /bin/bash

security-scan: ## Run security scans (if tools available)
	@echo "Running security scans..."
	@if command -v bandit >/dev/null 2>&1; then \
		cd glue-jobs && bandit -r src/; \
	else \
		echo "⚠️  bandit not installed, skipping security scan"; \
	fi

docs: ## Generate documentation (if sphinx is available)
	@echo "Documentation can be found in:"
	@echo "  - README.md (main documentation)"
	@echo "  - runbooks/ (operational guides)"
	@echo "  - glue-jobs/src/ (code documentation)"

health-check: ## Run basic health checks
	@echo "Running health checks..."
	@echo "✅ Checking Docker..."
	@docker --version >/dev/null && echo "  Docker is available" || echo "  ❌ Docker not available"
	@echo "✅ Checking AWS CLI..."
	@aws --version >/dev/null && echo "  AWS CLI is available" || echo "  ❌ AWS CLI not available"
	@echo "✅ Checking Terraform..."
	@terraform -version >/dev/null && echo "  Terraform is available" || echo "  ❌ Terraform not available"
	@echo "✅ Checking UV..."
	@uv --version >/dev/null && echo "  UV is available" || echo "  ❌ UV not available"
	@echo "✅ Checking Python..."
	@python3 --version >/dev/null && echo "  Python 3 is available" || echo "  ❌ Python 3 not available"

ci-test: ## Run tests as they would run in CI
	@echo "Running CI test suite..."
	@$(MAKE) lint
	@$(MAKE) type-check
	@$(MAKE) test-unit
	@$(MAKE) validate-terraform
	@echo "✅ All CI checks passed!"

dev-setup: ## Complete development setup
	@echo "Setting up development environment..."
	@$(MAKE) setup
	@$(MAKE) health-check
	@echo ""
	@echo "🎉 Development environment is ready!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Review the README.md file"
	@echo "  2. Configure AWS credentials: aws configure"
	@echo "  3. Run a test job: make run-local JOB=customer_import"
	@echo "  4. Review the runbooks in runbooks/"

# Development workflow helpers
dev-test: format lint test-unit ## Quick development test cycle

dev-package-test: ## Package and test locally
	@$(MAKE) package
	@$(MAKE) run-local JOB=customer_import

# Environment-specific shortcuts
dev: ## Deploy to dev environment
	@$(MAKE) deploy-all ENV=dev

staging: ## Deploy to staging environment  
	@$(MAKE) deploy-all ENV=staging

prod: ## Deploy to prod environment (requires confirmation)
	@echo "⚠️  You are about to deploy to PRODUCTION!"
	@read -p "Are you sure? (y/N): " confirm && [ $$confirm = "y" ]
	@$(MAKE) deploy-all ENV=prod

.DEFAULT_GOAL := help