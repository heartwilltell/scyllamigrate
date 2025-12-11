.PHONY: help build test test-unit test-integration lint fmt vet clean install deps docker-up docker-down docker-logs check ci test-all coverage

# Variables
BINARY_NAME=scyllamigrate
CMD_PATH=./cmd
COVERAGE_FILE=coverage.out
COVERAGE_HTML=coverage.html

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the CLI binary
	@echo "Building $(BINARY_NAME)..."
	@go build -ldflags="-s -w" -o $(BINARY_NAME) $(CMD_PATH)
	@echo "Build complete: $(BINARY_NAME)"

install: ## Install the CLI tool
	@echo "Installing $(BINARY_NAME)..."
	@go install $(CMD_PATH)
	@echo "Installation complete"

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies updated"

test: test-unit ## Run all unit tests

test-unit: ## Run unit tests
	@echo "Running unit tests..."
	@go test -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...

test-integration: docker-up ## Run integration tests (requires docker-compose)
	@echo "Running integration tests..."
	@SCYLLA_HOSTS=localhost:9042 SCYLLA_KEYSPACE=test_migrations go test -v -run "TestIntegration" ./...
	@$(MAKE) docker-down

test-all: test-unit docker-up ## Run all tests including integration tests
	@echo "Running all tests..."
	@SCYLLA_HOSTS=localhost:9042 SCYLLA_KEYSPACE=test_migrations go test -v -race -run "TestIntegration" ./...
	@$(MAKE) docker-down

coverage: test-unit ## Generate test coverage report
	@echo "Generating coverage report..."
	@go tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"
	@go tool cover -func=$(COVERAGE_FILE)

lint: ## Run linters
	@echo "Running linters..."
	@golangci-lint run

fmt: ## Format Go code
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Code formatted"

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...
	@echo "Vet complete"

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -f $(BINARY_NAME)
	@rm -f $(COVERAGE_FILE)
	@rm -f $(COVERAGE_HTML)
	@go clean
	@echo "Clean complete"

docker-up: ## Start ScyllaDB with docker-compose
	@echo "Starting ScyllaDB..."
	@docker compose up -d
	@echo "Waiting for ScyllaDB to be ready..."
	@timeout=60; \
	while [ $$timeout -gt 0 ]; do \
		if docker exec scyllamigrate-test cqlsh -e "SELECT now() FROM system.local" 2>/dev/null; then \
			echo "ScyllaDB is ready"; \
			break; \
		fi; \
		echo "Waiting for ScyllaDB... ($$timeout seconds remaining)"; \
		sleep 2; \
		timeout=$$((timeout - 2)); \
	done; \
	if [ $$timeout -le 0 ]; then \
		echo "ScyllaDB failed to start"; \
		exit 1; \
	fi
	@docker exec scyllamigrate-test cqlsh -e "CREATE KEYSPACE IF NOT EXISTS test_migrations WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};" 2>/dev/null || true

docker-down: ## Stop ScyllaDB
	@echo "Stopping ScyllaDB..."
	@docker compose down
	@echo "ScyllaDB stopped"

docker-logs: ## Show ScyllaDB logs
	@docker compose logs -f scylla

check: fmt vet lint ## Run all checks (format, vet, lint)

ci: deps check test-all ## Run CI pipeline locally

