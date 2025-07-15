-- Revised ClickHouse Schema for Optimized Queries

-- ClickHouse Archive Schema: Complete State Access History
-- This schema stores ALL state access events, not just the latest access

-- Accounts Archive Table: Store every account access event
CREATE TABLE accounts_archive (
    address FixedString(20),        -- 20-byte Ethereum address (binary format)
    block_number UInt64,            -- Block number when accessed
    is_contract UInt8               -- Boolean: 1 for contract, 0 for EOA
) ENGINE = MergeTree()
ORDER BY (block_number, address)    -- Primary key optimized for block-based range queries
PARTITION BY intDiv(block_number, 1000000)  -- Partition by millions of blocks for efficient queries
SETTINGS index_granularity = 8192; -- Default granularity for optimal performance

-- Storage Archive Table: Store every storage slot access event  
CREATE TABLE storage_archive (
    address FixedString(20),        -- 20-byte Ethereum address (binary format)
    slot_key FixedString(32),       -- 32-byte storage slot key (binary format)
    block_number UInt64             -- Block number when accessed
) ENGINE = MergeTree()
ORDER BY (block_number, address, slot_key)  -- Primary key optimized for block-based and address queries
PARTITION BY intDiv(block_number, 1000000)   -- Partition by millions of blocks for efficient queries
SETTINGS index_granularity = 8192;          -- Default granularity for optimal performance

-- Metadata Table: Store system metadata for tracking progress
CREATE TABLE metadata_archive (
    key String,
    value String,
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key;

-- Initialize metadata for tracking last indexed range
INSERT INTO metadata_archive (key, value) VALUES ('last_indexed_range', '0');

-- Secondary indexes for performance optimization on common query patterns

-- Index for account type filtering (frequently used in analytics)
ALTER TABLE accounts_archive ADD INDEX idx_is_contract is_contract TYPE minmax GRANULARITY 4;

-- Index for address-based queries (used in specific account lookups)  
ALTER TABLE accounts_archive ADD INDEX idx_address address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE storage_archive ADD INDEX idx_address address TYPE bloom_filter GRANULARITY 4;

-- Index for storage slot queries
ALTER TABLE storage_archive ADD INDEX idx_slot_key slot_key TYPE bloom_filter GRANULARITY 4;

-- 1. State Tables (current/latest access per address/slot)

CREATE TABLE accounts_state (
    address            FixedString(20),
    is_contract        UInt8,
    last_access_block  UInt64
) ENGINE = ReplacingMergeTree(last_access_block)
PARTITION BY intDiv(last_access_block, 1000000)      -- 1M‐block partitions
ORDER BY (address)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mv_accounts_state
TO accounts_state AS
SELECT
    address,
    argMax(is_contract, block_number)   AS is_contract,
    max(block_number)                   AS last_access_block
FROM accounts_archive
GROUP BY address;


CREATE TABLE storage_state (
    address            FixedString(20),
    slot_key           FixedString(32),
    last_access_block  UInt64
) ENGINE = ReplacingMergeTree(last_access_block)
PARTITION BY intDiv(last_access_block, 1000000)      -- same 1M‐block partitioning
ORDER BY (address, slot_key)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mv_storage_state
TO storage_state AS
SELECT
    address,
    slot_key,
    max(block_number) AS last_access_block
FROM storage_archive
GROUP BY address, slot_key;


-- 2. Secondary Indexes on State Tables to Speed Filters

ALTER TABLE accounts_state
    ADD INDEX idx_acc_last_access   last_access_block TYPE minmax   GRANULARITY 4,
    ADD INDEX idx_acc_address       address           TYPE bloom_filter GRANULARITY 4;

ALTER TABLE storage_state
    ADD INDEX idx_st_last_access    last_access_block TYPE minmax   GRANULARITY 4,
    ADD INDEX idx_st_address        address           TYPE bloom_filter GRANULARITY 4;


-- 3. Access-Count Aggregates (for "accessed once" queries)

CREATE TABLE account_access_count_agg (
    address       FixedString(20),
    is_contract_state   AggregateFunction(argMax, UInt8, UInt64),
    access_count  AggregateFunction(count, UInt64)
) 
ENGINE = AggregatingMergeTree()
ORDER BY (address);

CREATE MATERIALIZED VIEW mv_account_access_count
TO account_access_count_agg AS
SELECT
    address,
    argMaxState(is_contract, block_number) AS is_contract_state,
    countState() AS access_count
FROM accounts_archive
GROUP BY address;


CREATE TABLE storage_access_count_agg (
    address       FixedString(20),
    slot_key      FixedString(32),
    access_count  AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (address, slot_key);

CREATE MATERIALIZED VIEW mv_storage_access_count
TO storage_access_count_agg AS
SELECT
    address,
    slot_key,
    countState() AS access_count
FROM storage_archive
GROUP BY address, slot_key;


-- 4. Per-Block Access Summaries (for time-series & "highest blocks")

CREATE TABLE accounts_block_summary (
    block_number       UInt64,
    eoa_accesses       UInt64,
    contract_accesses  UInt64
) ENGINE = SummingMergeTree()
ORDER BY (block_number);

CREATE MATERIALIZED VIEW mv_accounts_block_summary
TO accounts_block_summary AS
SELECT
    block_number,
    sum(if(is_contract = 0, 1, 0)) AS eoa_accesses,
    sum(if(is_contract = 1, 1, 0)) AS contract_accesses
FROM accounts_archive
GROUP BY block_number;


CREATE TABLE storage_block_summary (
    block_number     UInt64,
    storage_accesses UInt64
) ENGINE = SummingMergeTree()
ORDER BY (block_number);

CREATE MATERIALIZED VIEW mv_storage_block_summary
TO storage_block_summary AS
SELECT
    block_number,
    count() AS storage_accesses
FROM storage_archive
GROUP BY block_number;


-- 5. Contract-Level Storage Slot Counts (for top-10 & per-contract stats)

CREATE TABLE contract_storage_count_agg (
    address     FixedString(20),
    total_slots AggregateFunction(uniq, FixedString(32))
) ENGINE = AggregatingMergeTree()
ORDER BY (address);

CREATE MATERIALIZED VIEW mv_contract_storage_count
TO contract_storage_count_agg AS
SELECT
    address,
    uniqState(slot_key) AS total_slots -- uniqState has some error rate (~1-2%)
FROM storage_archive
GROUP BY address;
