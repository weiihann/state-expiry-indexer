-- Drop ClickHouse Archive Schema Tables and Views
-- This migration reverses the complete archive schema setup

-- Drop regular views
DROP VIEW IF EXISTS latest_storage_access;
DROP VIEW IF EXISTS latest_account_access;

-- Drop indexes (in reverse order)
ALTER TABLE storage_archive DROP INDEX IF EXISTS idx_slot_key;
ALTER TABLE storage_archive DROP INDEX IF EXISTS idx_address;
ALTER TABLE accounts_archive DROP INDEX IF EXISTS idx_address;
ALTER TABLE accounts_archive DROP INDEX IF EXISTS idx_is_contract;

-- Drop tables
DROP TABLE IF EXISTS metadata_archive;
DROP TABLE IF EXISTS storage_archive;
DROP TABLE IF EXISTS accounts_archive; 