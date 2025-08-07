.PHONY: help setup test lint format run-local package deploy-infra deploy-jobs clean install-dev check-deps

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies and setup development environment
	@echo "Setting up development environment..."
	cd glue-jobs && poetry install
	pre-commit install
	@echo "✅ Setup completed!"

install-dev: ## Install development dependencies only
	cd glue-jobs && poetry install --only=dev

check-deps: ## Check for dependency updates
	cd glue-jobs && poetry show --outdated

test: test-unit test-int ## Run all tests

test-unit: ## Run unit tests with coverage
	@echo "Running unit tests..."
	cd glue-jobs && poetry run pytest tests/unit -v --cov=src --cov-report=term-missing

test-int: ## Run integration tests in Docker
	@echo "Running integration tests..."
	docker-compose up -d glue
	@echo "Waiting for container to be ready..."
	@sleep 10
	docker exec glue-local pytest /home/glue_user/workspace/tests/integration -v
	docker-compose down

test-watch: ## Run tests in watch mode
	cd glue-jobs && poetry run pytest-watch tests/unit

lint: ## Run linting checks
	@echo "Running linting checks..."
	cd glue-jobs && poetry run ruff check .
	@echo "✅ Linting passed!"

format: ## Format code with black and ruff
	@echo "Formatting code..."
	cd glue-jobs && poetry run black .
	cd glue-jobs && poetry run ruff format .
	@echo "✅ Code formatted!"

type-check: ## Run type checking with mypy
	cd glue-jobs && poetry run mypy src/

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

run-local: ## Run job locally (usage: make run-local JOB=customer_import)
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Error: Please specify JOB parameter"; \
		echo "Usage: make run-local JOB=customer_import"; \
		exit 1; \
	fi
	@echo "Running $(JOB) locally..."
	@$(MAKE) package
	docker-compose up -d glue
	@echo "Waiting for container to be ready..."
	@sleep 10
	./glue-jobs/scripts/run_local.sh $(JOB)

run-all-local: ## Run all jobs locally for testing
	@echo "Running all jobs locally..."
	@$(MAKE) run-local JOB=customer_import
	@$(MAKE) run-local JOB=sales_etl
	@$(MAKE) run-local JOB=inventory_sync
	@$(MAKE) run-local JOB=product_catalog

package: ## Package jobs for deployment
	@echo "Packaging Glue jobs..."
	@mkdir -p dist
	cd glue-jobs && ./scripts/package.sh
	@echo "✅ Package created in dist/utils.zip"

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
	docker-compose down --volumes --remove-orphans 2>/dev/null || true
	@echo "✅ Cleanup completed!"

docker-up: ## Start Docker containers
	docker-compose up -d

docker-down: ## Stop Docker containers
	docker-compose down

docker-logs: ## View Docker container logs
	docker-compose logs -f glue

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
	@echo "✅ Checking Poetry..."
	@poetry --version >/dev/null && echo "  Poetry is available" || echo "  ❌ Poetry not available"
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