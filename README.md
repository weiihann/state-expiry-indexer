# State Expiry Indexer

A comprehensive system designed to track and analyze Ethereum state access patterns to identify expired state.

## Quick Start

### Building the Application

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

# Check migration status
make migrate-status

# Start the full indexer and API server
./bin/state-expiry-indexer run
```

## Logging Features

The application supports advanced logging with colors and structured output:

### Log Levels
- `debug`: Detailed debugging information
- `info`: General operational information  
- `warn`: Warning messages for recoverable issues
- `error`: Error messages for failures

### Log Formats
- `text`: Human-readable format (default)
- `json`: Structured JSON format for production

### Color Support
- Automatic color detection for terminal output
- Colors can be disabled with `--no-color` flag
- Different colors for each log level:
  - **Blue**: Info messages
  - **Yellow**: Warning messages  
  - **Red**: Error messages
  - **Gray**: Debug messages

### Examples

```bash
# Colored output (default)
./bin/state-expiry-indexer migrate status

# Disable colors
./bin/state-expiry-indexer --no-color migrate status

# JSON format for production
./bin/state-expiry-indexer --log-format json migrate status

# Debug level with colors
./bin/state-expiry-indexer --log-level debug migrate status

# Error level only
./bin/state-expiry-indexer --log-level error migrate status
```

## Database Management

```bash
# Start PostgreSQL with Docker
make db-up

# Stop PostgreSQL
make db-down

# View database logs
make db-logs

# Check migration status
make migrate-status
```

## Development Workflow

```bash
# Set up development environment
make dev-setup

# Run development checks (format, vet, test)
make dev-check

# Run full CI pipeline
make ci

# Format code
make fmt

# Run tests
make test

# Run tests with coverage
make test-coverage

# Clean build artifacts
make clean
```

## Available Make Targets

Run `make help` to see all available targets:

- **Build**: `build`, `build-dev`, `install`
- **Test**: `test`, `test-race`, `test-coverage`, `bench`
- **Development**: `run`, `run-help`, `migrate-status`
- **Database**: `db-up`, `db-down`, `db-logs`
- **Code Quality**: `fmt`, `vet`, `lint`, `tidy`
- **Clean**: `clean`, `clean-all`
- **CI/CD**: `ci`, `dev-check`, `version`

## Configuration

The application supports configuration via:

- Environment variables
- Configuration files in `./configs/config.env`
- Command-line flags

### Required Environment Variables

```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=state_expiry
RPC_URL=https://your-ethereum-rpc-url
API_PORT=8080
```

## API Endpoints

Once running, the API server provides these endpoints:

- `GET /api/v1/stats/expired-count?expiry_block=<block>`
- `GET /api/v1/stats/top-expired-contracts?expiry_block=<block>&n=<count>`
- `GET /api/v1/lookup?address=<address>&slot=<slot>`

## Architecture

The application consists of:

1. **RPC Client**: Downloads state diffs from Ethereum
2. **File Storage**: Saves state diffs as JSON files
3. **Indexer**: Processes state diffs and updates database
4. **API Server**: Serves queries about state access patterns
5. **Database**: PostgreSQL with partitioned tables for performance

## Dependencies

- Go 1.24+
- PostgreSQL 13+
- Docker & Docker Compose (for development)
- Make (for build automation) 