# State Expiry Indexer Documentation

## Overview

The State Expiry Indexer is a comprehensive system for tracking and analyzing Ethereum state access patterns. It provides two operational modes:

1. **Default Mode (PostgreSQL)**: Tracks latest state access for current state analysis
2. **Archive Mode (ClickHouse)**: Stores complete state access history for temporal analysis

## 📁 Documentation Structure

### Core Documentation

- **[Archive System Guide](ARCHIVE_SYSTEM.md)** - Comprehensive guide to the ClickHouse archive system
- **[ClickHouse Schema](../db/ch-migrations/README.md)** - Database schema and optimization documentation
- **[Configuration Guide](../configs/config.env.example)** - Complete configuration reference

### Technical Documentation

- **[API Reference](../internal/api/)** - REST API endpoints and responses
- **[Repository Interface](../internal/repository/interface.go)** - Database abstraction layer
- **[Testing Framework](../internal/repository/archive_test.go)** - Archive system testing

### Operational Tools

- **[Benchmark Script](../scripts/archive_benchmark.go)** - Performance comparison tool
- **[Migration System](../cmd/migrate.go)** - Database migration management
- **[CLI Commands](../cmd/)** - Complete command-line interface

## 🚀 Quick Start

### Default Mode (PostgreSQL)

```bash
# Configure PostgreSQL
cp configs/config.env.example configs/config.env
# Edit PostgreSQL settings

# Run migrations and start indexer
./bin/state-expiry-indexer migrate up
./bin/state-expiry-indexer run
```

### Archive Mode (ClickHouse)

```bash
# Configure ClickHouse
cp configs/config.env.example configs/config.env
# Edit ClickHouse settings

# Run ClickHouse migrations and start archive indexer
./bin/state-expiry-indexer migrate ch up
./bin/state-expiry-indexer run --archive
```

## 🏗️ System Architecture

### Data Flow

```
Ethereum RPC → State Diffs → Indexer → Database → Analytics API
                     ↓
              File Storage (JSON/Compressed)
```

### Database Architecture

#### PostgreSQL (Default)
- **Pattern**: UPSERT - Latest access only
- **Tables**: `accounts_current`, `storage_current`
- **Use Case**: Current state analysis and expiry detection

#### ClickHouse (Archive)  
- **Pattern**: INSERT - Complete access history
- **Tables**: `accounts_archive`, `storage_archive`
- **Use Case**: Historical analysis and temporal trends

## 📊 Performance Comparison

| Metric | PostgreSQL | ClickHouse Archive |
|--------|-----------|-------------------|
| **Analytics Queries** | 5-15 seconds | 1-3 seconds |
| **Storage Size** | ~50GB (21M blocks) | ~500GB (21M blocks, compressed) |
| **Query Types** | Current state only | Current state + historical |
| **Compression** | None | ~70% (LZ4) |

## 🔧 Configuration

### Environment Variables

#### Shared Configuration
```bash
# RPC Configuration
RPC_URLS=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
RPC_TIMEOUT_SECONDS=30

# Indexer Configuration  
BLOCK_BATCH_SIZE=100
POLL_INTERVAL_SECONDS=5
RANGE_SIZE=1000

# API Configuration
API_HOST=localhost
API_PORT=8080
```

#### PostgreSQL Configuration
```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=state_expiry
DB_MAX_CONNS=10
DB_MIN_CONNS=2
```

#### ClickHouse Configuration
```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=state_expiry
CLICKHOUSE_MAX_CONNS=10
CLICKHOUSE_MIN_CONNS=2
```

## 🧪 Testing

### Test Categories

1. **Unit Tests**: Individual component testing
   ```bash
   go test ./pkg/... 
   go test ./internal/...
   ```

2. **Archive Equivalence Tests**: Verify PostgreSQL/ClickHouse produce identical results
   ```bash
   go test ./internal/repository -run TestArchiveEquivalence
   ```

3. **Performance Tests**: Benchmark query performance
   ```bash
   go test ./internal/repository -run TestArchivePerformance -timeout 10m
   ```

4. **Integration Tests**: End-to-end functionality
   ```bash
   go test ./internal/repository -run TestArchiveDataIntegrity
   ```

### Benchmark Tool

```bash
# Compare PostgreSQL vs ClickHouse performance
cd scripts
go run archive_benchmark.go

# View help
go run archive_benchmark.go --help
```

## 🗄️ Database Operations

### Migration Commands

#### PostgreSQL Migrations
```bash
./bin/state-expiry-indexer migrate up      # Apply all migrations
./bin/state-expiry-indexer migrate down    # Rollback one migration  
./bin/state-expiry-indexer migrate status  # Show status
./bin/state-expiry-indexer migrate version # Show version
```

#### ClickHouse Migrations
```bash
./bin/state-expiry-indexer migrate ch up      # Apply ClickHouse migrations
./bin/state-expiry-indexer migrate ch down    # Rollback ClickHouse migration
./bin/state-expiry-indexer migrate ch status  # Show ClickHouse status
./bin/state-expiry-indexer migrate ch version # Show ClickHouse version
```

### Data Management

#### Compression Operations
```bash
# Compress existing JSON files
./bin/state-expiry-indexer compress --all

# Compress specific range
./bin/state-expiry-indexer compress --start-block 1000000 --end-block 2000000

# Preview compression (dry run)
./bin/state-expiry-indexer compress --all --dry-run
```

#### Range Merging (Filesystem Optimization)
```bash
# Merge individual files into compressed ranges
./bin/state-expiry-indexer merge --start-block 1000000 --end-block 2000000

# Custom range size
./bin/state-expiry-indexer merge --start-block 1000000 --end-block 2000000 --range-size 500

# Preview merge operation
./bin/state-expiry-indexer merge --start-block 1000000 --end-block 2000000 --dry-run
```

## 🌐 API Reference

### Core Endpoints

#### Analytics Dashboard
```bash
GET /api/v1/stats/analytics?expiry_block=20000000
```
Returns comprehensive analytics with 7 key metrics:
- Account expiry analysis (EOA vs Contract)
- Storage slot expiry statistics
- Top expired contracts
- Distribution analysis

#### State Queries
```bash
GET /api/v1/state/{address}/last-access
GET /api/v1/state/{address}/{slot}/last-access
GET /api/v1/account/{address}/info
```

#### System Status
```bash
GET /api/v1/sync/status
GET /api/v1/health
```

### Response Format

All endpoints return JSON with consistent structure:
```json
{
  "success": true,
  "data": { /* endpoint-specific data */ },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## 🔍 Monitoring and Observability

### Logging

The system uses structured logging with configurable levels:
```bash
# Debug logging
LOG_LEVEL=debug ./bin/state-expiry-indexer run

# Production logging  
LOG_LEVEL=info ./bin/state-expiry-indexer run
```

### Metrics

#### Key Performance Indicators
- **Indexing Rate**: Blocks processed per second
- **API Response Time**: Analytics query duration
- **Database Size**: Storage utilization
- **Compression Ratio**: Storage efficiency

#### Health Monitoring
```bash
# Check indexer progress
curl "http://localhost:8080/api/v1/sync/status"

# Verify database connectivity
./bin/state-expiry-indexer migrate status  # PostgreSQL
./bin/state-expiry-indexer migrate ch status  # ClickHouse
```

## 🔧 Troubleshooting

### Common Issues

#### Connection Problems
```bash
# Test database connectivity
./bin/state-expiry-indexer migrate status

# Check configuration
./bin/state-expiry-indexer run --dry-run
```

#### Performance Issues
```bash
# Run performance benchmarks
go run scripts/archive_benchmark.go

# Enable debug logging
LOG_LEVEL=debug ./bin/state-expiry-indexer run --archive
```

#### Data Inconsistency
```bash
# Run equivalence tests
go test ./internal/repository -run TestArchiveEquivalence

# Manual verification
curl "http://localhost:8080/api/v1/stats/analytics?expiry_block=20000000"
```

### Debug Mode

```bash
# Enable comprehensive debugging
DEBUG=1 LOG_LEVEL=debug ./bin/state-expiry-indexer run --archive
```

## 📈 Performance Optimization

### ClickHouse Optimizations

#### Query Performance
- **Primary Key**: Optimized for block-range queries
- **Partitioning**: 1M block partitions for efficient pruning
- **Compression**: LZ4 compression for storage efficiency
- **Views**: Pre-aggregated latest access views

#### Storage Optimization
```sql
-- Periodic optimization (run monthly)
OPTIMIZE TABLE accounts_archive FINAL;
OPTIMIZE TABLE storage_archive FINAL;

-- Update statistics
ANALYZE TABLE accounts_archive;
ANALYZE TABLE storage_archive;
```

### PostgreSQL Optimizations

#### Index Maintenance
```sql
-- Reindex for performance
REINDEX TABLE accounts_current;
REINDEX TABLE storage_current;

-- Update statistics
ANALYZE accounts_current;
ANALYZE storage_current;
```

## 🔄 Migration Guide

### PostgreSQL to Archive Mode

#### Option A: Fresh Start (Recommended)
1. Setup ClickHouse server
2. Run ClickHouse migrations
3. Start archive indexer from genesis
4. Verify data consistency

#### Option B: Data Migration
1. Export PostgreSQL data
2. Transform to archive format
3. Import into ClickHouse
4. Resume indexing

### Rollback Strategy
- PostgreSQL data remains unchanged during archive operation
- Switch back by removing `--archive` flag
- No data loss risk

## 📚 Additional Resources

### External Documentation
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Go Testing Framework](https://golang.org/pkg/testing/)

### Development Resources
- [Contributing Guidelines](../CONTRIBUTING.md)
- [Code Architecture](../internal/README.md)
- [Build Instructions](../Makefile)

### Community
- GitHub Issues: Bug reports and feature requests
- GitHub Discussions: Architecture questions and usage help

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

---

**Need Help?** Check the [Archive System Guide](ARCHIVE_SYSTEM.md) for detailed operational instructions or run any command with `--help` for usage information. 