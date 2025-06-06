# Makefile for State Expiry Indexer

# Variables
BINARY_NAME=state-expiry-indexer
BINARY_DIR=bin
BUILD_DIR=.
GO_FILES=$(shell find . -name "*.go" -type f)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

# Default target
.PHONY: help
help: ## Show this help message
	@echo "State Expiry Indexer - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# Build targets
.PHONY: build
build: $(BINARY_DIR)/$(BINARY_NAME) ## Build the binary

$(BINARY_DIR)/$(BINARY_NAME): $(GO_FILES)
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BINARY_DIR)
	go build $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME) $(BUILD_DIR)

.PHONY: build-dev
build-dev: ## Build the binary for development (no optimizations)
	@echo "Building $(BINARY_NAME) for development..."
	@mkdir -p $(BINARY_DIR)
	go build -race -o $(BINARY_DIR)/$(BINARY_NAME) $(BUILD_DIR)

.PHONY: install
install: ## Install the binary to $GOPATH/bin
	@echo "Installing $(BINARY_NAME)..."
	go install $(LDFLAGS) $(BUILD_DIR)

# Test targets
.PHONY: test
test: ## Run all tests
	@echo "Running tests..."
	go test -v ./...

.PHONY: test-race
test-race: ## Run tests with race detection
	@echo "Running tests with race detection..."
	go test -race -v ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: bench
bench: ## Run benchmarks
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Development targets
.PHONY: run
run: build ## Build and run the application
	@echo "Running $(BINARY_NAME)..."
	./$(BINARY_DIR)/$(BINARY_NAME)

.PHONY: run-help
run-help: build ## Build and show help
	./$(BINARY_DIR)/$(BINARY_NAME) --help

.PHONY: migrate-status
migrate-status: build ## Check migration status
	./$(BINARY_DIR)/$(BINARY_NAME) migrate status

# Database targets
.PHONY: db-up
db-up: ## Start database with docker-compose
	@echo "Starting database..."
	docker-compose up -d postgres

.PHONY: db-down
db-down: ## Stop database
	@echo "Stopping database..."
	docker-compose down

.PHONY: db-logs
db-logs: ## Show database logs
	docker-compose logs -f postgres

# Code quality targets
.PHONY: fmt
fmt: ## Format Go code
	@echo "Formatting code..."
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (requires golangci-lint installation)
	@echo "Running golangci-lint..."
	golangci-lint run

.PHONY: tidy
tidy: ## Tidy up go modules
	@echo "Tidying go modules..."
	go mod tidy

# Clean targets
.PHONY: clean
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	rm -rf $(BINARY_DIR)
	rm -f coverage.out coverage.html

.PHONY: clean-all
clean-all: clean ## Clean all artifacts including Docker volumes
	@echo "Cleaning all artifacts..."
	docker-compose down -v --remove-orphans 2>/dev/null || true

# Docker targets
.PHONY: docker-build
docker-build: ## Build Docker image (if Dockerfile exists)
	@if [ -f Dockerfile ]; then \
		echo "Building Docker image..."; \
		docker build -t $(BINARY_NAME):$(VERSION) .; \
	else \
		echo "Dockerfile not found, skipping Docker build"; \
	fi

# Release targets
.PHONY: release-dry
release-dry: ## Show what would be released
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Would build: $(BINARY_NAME)"

# Development workflow
.PHONY: dev-setup
dev-setup: ## Set up development environment
	@echo "Setting up development environment..."
	go mod download
	@echo "Installing development tools..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)

.PHONY: dev-check
dev-check: fmt vet test ## Run development checks (format, vet, test)
	@echo "Development checks completed successfully!"

# Full build and test pipeline
.PHONY: ci
ci: tidy fmt vet test build ## Run full CI pipeline
	@echo "CI pipeline completed successfully!"

# Show build info
.PHONY: version
version: ## Show version information
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)" 