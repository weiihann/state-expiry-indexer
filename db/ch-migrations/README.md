# ClickHouse Archive Schema Documentation

## Overview

The ClickHouse archive system stores **complete state access history** for Ethereum state expiry analysis. Unlike the PostgreSQL system which stores only the latest access block for each account/storage slot, the archive system stores **every single access event**.

## Architecture Design

### Schema Differences: PostgreSQL vs ClickHouse

| Aspect | PostgreSQL (Current) | ClickHouse (Archive) |
|--------|---------------------|---------------------|
| **Data Pattern** | UPSERT - Latest access only | INSERT - All access events |
| **Storage Model** | `last_access_block` per address/slot | Multiple records per address/slot |
| **Example** | Account accessed on blocks [10, 100, 1000] → stores block 1000 only | Account accessed on blocks [10, 100, 1000] → stores 3 records |
| **Use Case** | Current state queries | Historical analysis, complete audit trail |

### Table Design

#### accounts_archive
```sql
CREATE TABLE accounts_archive (
    address FixedString(20),        -- Binary Ethereum address (efficient storage)
    block_number UInt64,            -- Block number when accessed  
    is_contract UInt8               -- Contract flag (0=EOA, 1=Contract)
) ENGINE = MergeTree()
ORDER BY (block_number, address)    -- Optimized for block-range queries
PARTITION BY intDiv(block_number, 1000000);  -- Partition by millions of blocks
```

#### storage_archive  
```sql
CREATE TABLE storage_archive (
    address FixedString(20),        -- Binary Ethereum address
    slot_key FixedString(32),       -- Binary storage slot key
    block_number UInt64             -- Block number when accessed
) ENGINE = MergeTree()
ORDER BY (block_number, address, slot_key)  -- Optimized for queries
PARTITION BY intDiv(block_number, 1000000);  -- Partition by millions of blocks
```

## Query Optimization Strategy

### 1. Primary Key Design
- **ORDER BY (block_number, address)**: Optimizes the most common query pattern (time-based filtering)
- **Time-range queries**: `WHERE block_number < expiry_block` leverage sparse primary index
- **Address lookups**: Secondary sorting by address improves address-specific queries

### 2. Partitioning Strategy
- **Block-based partitioning**: `PARTITION BY intDiv(block_number, 1000000)`
- **Benefits**: Efficient pruning for block-range queries, logical data organization by block height
- **Query performance**: Queries with block number filters only scan relevant partitions (1M block ranges)

### 3. Secondary Indexes
```sql
-- Account type filtering (frequently used in analytics)
ALTER TABLE accounts_archive ADD INDEX idx_is_contract is_contract TYPE minmax GRANULARITY 4;

-- Address-based queries optimization
ALTER TABLE accounts_archive ADD INDEX idx_address address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE storage_archive ADD INDEX idx_address address TYPE bloom_filter GRANULARITY 4;

-- Storage slot queries optimization  
ALTER TABLE storage_archive ADD INDEX idx_slot_key slot_key TYPE bloom_filter GRANULARITY 4;
```

### 4. Analytics Views

#### Latest Access Views
Replicate PostgreSQL behavior for compatibility:
```sql
-- Get latest access per account (equivalent to PostgreSQL accounts_current)
CREATE VIEW latest_account_access AS
SELECT 
    address,
    argMax(block_number, block_number) as last_access_block,
    argMax(is_contract, block_number) as is_contract
FROM accounts_archive
GROUP BY address;
```

#### Future Optimization: Materialized Views
Materialized views can be added later for pre-aggregated analytics performance when needed.

## Query Pattern Adaptation

### Current PostgreSQL Analytics → ClickHouse Archive

1. **Expired Account Count**
   ```sql
   -- PostgreSQL (current)
   SELECT COUNT(*) FROM accounts_current WHERE last_access_block < expiry_block;
   
   -- ClickHouse (archive)  
   SELECT COUNT(*) FROM latest_account_access WHERE last_access_block < expiry_block;
   ```

2. **Top Expired Contracts**
   ```sql
   -- PostgreSQL (current)
   SELECT address, COUNT(*) as slot_count 
   FROM storage_current 
   WHERE last_access_block < expiry_block
   GROUP BY address
   ORDER BY slot_count DESC LIMIT 10;
   
   -- ClickHouse (archive)
   SELECT address, COUNT(*) as slot_count
   FROM latest_storage_access  
   WHERE last_access_block < expiry_block
   GROUP BY address
   ORDER BY slot_count DESC LIMIT 10;
   ```

3. **Historical Analysis (New Capability)**
   ```sql
   -- Analyze access patterns by block ranges (not possible in PostgreSQL system)
   SELECT 
       intDiv(block_number, 100000) as block_range,
       COUNT(*) as total_accesses,
       uniq(address) as unique_addresses
   FROM accounts_archive
   WHERE block_number BETWEEN start_block AND end_block
   GROUP BY block_range
   ORDER BY block_range;
   ```

## Performance Characteristics

### Expected Performance Benefits
- **Compression**: ClickHouse LZ4 compression reduces storage by ~70%
- **Parallel Processing**: Column-oriented storage enables parallel query execution
- **Partition Pruning**: Block-range queries only scan relevant partitions (1M block ranges)
- **Sparse Indexes**: Primary key design optimizes range queries on block_number

### Storage Estimates
- **PostgreSQL**: ~50GB for 21M blocks (latest access only)
- **ClickHouse**: ~500GB for 21M blocks (complete history, after compression)
- **Scaling**: ClickHouse handles TB-scale datasets efficiently

## Migration Usage

### ClickHouse Migration Commands
```bash
# Apply ClickHouse migrations
./bin/state-expiry-indexer migrate ch up

# Check ClickHouse migration status  
./bin/state-expiry-indexer migrate ch status

# Rollback ClickHouse migrations
./bin/state-expiry-indexer migrate ch down
```

### Archive Mode Usage
```bash
# Use PostgreSQL (default)
./bin/state-expiry-indexer run

# Use ClickHouse archive mode
./bin/state-expiry-indexer run --archive
```

## Data Types and Storage

### Address Storage Optimization
- **FixedString(20)**: Binary storage for Ethereum addresses (20 bytes vs 42 character hex strings)
- **Performance**: Binary comparisons faster than string comparisons
- **Storage**: 50% reduction in address storage space

### Block Number Strategy
- **UInt64**: Native 64-bit unsigned integer for block numbers
- **Primary Focus**: Block number is the primary temporal dimension for Ethereum state analysis
- **Partitioning**: Block-based partitioning (1M block ranges) for optimal query performance

## Migration Files

1. **0001_initial_archive_schema**: Core schema including tables, indexes, and essential views

The migration includes both `.up.sql` and `.down.sql` for complete reversibility. Materialized views can be added later through separate migrations when optimization is needed. 