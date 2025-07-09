-- Drop ClickHouse Archive Schema Tables and Views
-- This migration reverses the complete optimized archive schema setup

-- Drop materialized views (in reverse dependency order)
DROP VIEW IF EXISTS mv_contract_storage_count;
DROP VIEW IF EXISTS mv_combined_block_summary_storage;
DROP VIEW IF EXISTS mv_combined_block_summary_accounts;
DROP VIEW IF EXISTS mv_storage_block_summary;
DROP VIEW IF EXISTS mv_accounts_block_summary;
DROP VIEW IF EXISTS mv_storage_access_count;
DROP VIEW IF EXISTS mv_account_access_count;
DROP VIEW IF EXISTS mv_storage_state;
DROP VIEW IF EXISTS mv_accounts_state;

-- Drop aggregation and summary tables
DROP TABLE IF EXISTS contract_storage_count_agg;
DROP TABLE IF EXISTS combined_block_summary;
DROP TABLE IF EXISTS storage_block_summary;
DROP TABLE IF EXISTS accounts_block_summary;
DROP TABLE IF EXISTS storage_access_count_agg;
DROP TABLE IF EXISTS account_access_count_agg;
DROP TABLE IF EXISTS storage_state;
DROP TABLE IF EXISTS accounts_state;

-- Drop indexes (in reverse order)
ALTER TABLE storage_archive DROP INDEX IF EXISTS idx_slot_key;
ALTER TABLE storage_archive DROP INDEX IF EXISTS idx_address;
ALTER TABLE accounts_archive DROP INDEX IF EXISTS idx_address;
ALTER TABLE accounts_archive DROP INDEX IF EXISTS idx_is_contract;

-- Drop tables
DROP TABLE IF EXISTS metadata_archive;
DROP TABLE IF EXISTS storage_archive;
DROP TABLE IF EXISTS accounts_archive;
