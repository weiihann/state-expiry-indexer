# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Build System
```bash
# Build the binary
make build

# Build for development (with race detection)
make build-dev

# Install to $GOPATH/bin
make install
```

### Running the Application
```bash
# Show all available commands
make help

# Build and run with default settings
make run

# Start database with Docker
make db-up

# Stop database
make db-down

# Check migration status
make migrate-status
```

### Testing and Quality
```bash
# Run all tests
make test

# Run tests with race detection
make test-race

# Run tests with coverage report
make test-coverage

# Run benchmarks
make bench

# Format Go code
make fmt

# Run go vet
make vet

# Run golangci-lint (requires installation)
make lint

# Tidy go modules
make tidy
```

### Development Workflow
```bash
# Set up development environment
make dev-setup

# Run development checks (format, vet, test)
make dev-check

# Run full CI pipeline
make ci

# Clean build artifacts
make clean
```

## Application Architecture

### Core Components
This is a **State Expiry Indexer** for Ethereum that tracks state access patterns to identify expired accounts using a modular three-component architecture:

1. **RPC Caller** (`pkg/rpc/`): Downloads state diffs from Ethereum nodes via RPC calls
2. **Indexer** (`internal/indexer/`): Processes state diff JSON files and updates PostgreSQL database
3. **API Server** (`internal/api/`): Serves HTTP queries about state access patterns
4. **Database Repository** (`internal/repository/`): PostgreSQL operations for state tracking
5. **File Storage** (`pkg/storage/`): Handles state diff JSON file storage

### Data Flow Pipeline
```
RPC Client → File Storage → Indexer → Database → API Server
(Downloads)   (JSON Files)  (Process)  (PostgreSQL) (HTTP Queries)
```

### CLI Commands Structure
- `run`: Main orchestrator running all components (RPC caller + indexer + API server)
- `run --download-only`: Run only RPC caller for data collection without database overhead
- `download`: Independent RPC caller process for data collection only
- `index`: Independent indexer process for data processing only  
- `migrate`: Database migration management using golang-migrate
- `genesis`: Process Ethereum genesis block initial state allocation

### Key Configuration
The application uses comprehensive configuration management with:
- Environment variables support
- Configuration files in `./configs/config.env`
- Command-line flag overrides
- Extensive validation for all parameters

Required environment variables:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=state_expiry
RPC_URL=https://your-ethereum-rpc-url
API_PORT=8080
```

### Database Schema
- Uses PostgreSQL with partitioned tables for performance
- Automated migrations using golang-migrate
- Tracks both current and historical state access patterns
- Metadata table for system state tracking

### Architectural Benefits
- **Fault Tolerance**: Components can fail and recover independently
- **Testing Independence**: Components can be tested in isolation
- **Scalability**: Each process can run at optimal speed
- **Recovery Capability**: Full replay scenarios without re-downloading data
- **Resource Efficiency**: Download-only mode for lightweight deployments

## Development Guidelines

### Component Testing Strategy
- **Repository Tests**: Use real PostgreSQL database for integration testing
- **RPC Client Tests**: Mock Ethereum RPC responses for unit testing  
- **Indexer Tests**: Use static test fixtures to avoid RPC dependencies
- **API Tests**: Test endpoints with real database and test data

### Error Handling
- Each component has independent error handling and retry logic
- Structured logging using Go's log/slog package with configurable levels
- Graceful shutdown handling across all concurrent workflows

### File Organization
- `cmd/`: CLI commands using Cobra framework
- `internal/`: Private application logic (config, indexer, api, database)
- `pkg/`: Public packages (rpc, storage, tracker, utils)
- `db/migrations/`: Database schema migrations
- `data/`: State diff files and genesis data
- `testdata/`: Test fixtures and mock data

### Performance Considerations
- Progress tracking every 1000 blocks or 8 seconds for operational visibility
- Batch processing for large datasets (genesis file has 8,893 accounts)
- Configurable intervals for different processing speeds
- Independent state tracking for downloads vs processing

### State Management
- **Download Tracker**: Tracks last downloaded block for RPC caller
- **Process Tracker**: Tracks last indexed block for database processing
- **Genesis Processing**: Handles initial state setup from genesis.json
- **Metadata Table**: System state and processing status tracking

### Multi-mode Operation
1. **Full Mode**: Complete pipeline (download + process + serve)
2. **Download-Only**: Lightweight data collection 
3. **Process-Only**: Batch processing of existing files
4. **API-Only**: Query server for existing data

This architecture enables flexible deployment scenarios from single-instance development to distributed production systems.