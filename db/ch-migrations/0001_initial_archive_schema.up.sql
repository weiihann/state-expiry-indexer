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

-- Analytics Views for Optimized Query Performance
-- These views optimize common analytics patterns

-- View: Latest Account Access per Address
-- This view finds the most recent access for each account, replicating PostgreSQL "latest" behavior
CREATE VIEW latest_account_access AS
SELECT 
    address,
    argMax(block_number, block_number) as last_access_block,
    argMax(is_contract, block_number) as is_contract
FROM accounts_archive
GROUP BY address;

-- View: Latest Storage Access per Address-Slot
-- This view finds the most recent access for each storage slot, replicating PostgreSQL "latest" behavior  
CREATE VIEW latest_storage_access AS
SELECT 
    address,
    slot_key,
    argMax(block_number, block_number) as last_access_block
FROM storage_archive
GROUP BY address, slot_key;

 