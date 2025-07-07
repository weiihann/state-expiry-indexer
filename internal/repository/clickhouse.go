package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

// ClickHouseRepository implements StateRepositoryInterface for ClickHouse archive mode
type ClickHouseRepository struct {
	db *sql.DB
}

// Ensure ClickHouseRepository implements StateRepositoryInterface
var _ StateRepositoryInterface = (*ClickHouseRepository)(nil)

func NewClickHouseRepository(db *sql.DB) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// Range-based processing methods (used by indexer)
func (r *ClickHouseRepository) GetLastIndexedRange(ctx context.Context) (uint64, error) {
	log := logger.GetLogger("clickhouse-repo")

	var value string
	query := "SELECT value FROM metadata_archive WHERE key = 'last_indexed_range'"
	err := r.db.QueryRowContext(ctx, query).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			// This can happen if the metadata table is empty. Assume we start from 0.
			log.Info("No last indexed range found, starting from 0")
			return 0, nil
		}
		log.Error("Could not get last indexed range", "error", err)
		return 0, fmt.Errorf("could not get last indexed range: %w", err)
	}

	var rangeNumber uint64
	if _, err := fmt.Sscanf(value, "%d", &rangeNumber); err != nil {
		log.Error("Could not parse last indexed range value", "value", value, "error", err)
		return 0, fmt.Errorf("could not parse last indexed range value '%s': %w", value, err)
	}

	log.Debug("Retrieved last indexed range", "range_number", rangeNumber)
	return rangeNumber, nil
}

func (r *ClickHouseRepository) UpdateRangeDataInTx(ctx context.Context, accounts map[string]uint64, accountType map[string]bool, storage map[string]map[string]uint64, rangeNumber uint64) error {
	log := logger.GetLogger("clickhouse-repo")

	log.Info("Starting ClickHouse range data update",
		"range_number", rangeNumber,
		"accounts_count", len(accounts),
		"storage_count", len(storage))

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("Could not begin transaction", "error", err)
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert account access events (archive mode stores ALL events, not just latest)
	if err := r.insertAccountAccessEventsInTx(ctx, tx, accounts, accountType); err != nil {
		log.Error("Could not insert account access events", "error", err)
		return fmt.Errorf("could not insert account access events: %w", err)
	}

	// Insert storage access events (archive mode stores ALL events, not just latest)
	if err := r.insertStorageAccessEventsInTx(ctx, tx, storage); err != nil {
		log.Error("Could not insert storage access events", "error", err)
		return fmt.Errorf("could not insert storage access events: %w", err)
	}

	// Update the last indexed range
	if err := r.updateLastIndexedRangeInTx(ctx, tx, rangeNumber); err != nil {
		log.Error("Could not update last indexed range", "error", err)
		return fmt.Errorf("could not update last indexed range: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Error("Could not commit transaction", "error", err)
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	log.Info("Successfully completed ClickHouse range data update",
		"range_number", rangeNumber,
		"accounts_inserted", len(accounts),
		"storage_slots_inserted", r.countStorageSlots(storage))

	return nil
}

// insertAccountAccessEventsInTx inserts ALL account access events for archive mode
func (r *ClickHouseRepository) insertAccountAccessEventsInTx(ctx context.Context, tx *sql.Tx, accounts map[string]uint64, accountType map[string]bool) error {
	if len(accounts) == 0 {
		return nil
	}

	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse INSERT statement for accounts_archive table
	query := `INSERT INTO accounts_archive (address, block_number, is_contract) VALUES `

	var values []interface{}
	var placeholders []string
	paramIdx := 1

	for address, blockNumber := range accounts {
		// Convert address to binary format (FixedString(20))
		addressBytes, err := utils.HexToBytes(address)
		if err != nil {
			log.Warn("Skipping invalid address", "address", address, "error", err)
			continue
		}

		// Get account type (default to false if not found)
		isContract := accountType[address]
		var isContractByte uint8
		if isContract {
			isContractByte = 1
		} else {
			isContractByte = 0
		}

		placeholders = append(placeholders, "(?, ?, ?)")
		values = append(values, addressBytes, blockNumber, isContractByte)
		paramIdx += 3
	}

	if len(placeholders) == 0 {
		return nil // No valid accounts to insert
	}

	fullQuery := query + strings.Join(placeholders, ", ")

	_, err := tx.ExecContext(ctx, fullQuery, values...)
	if err != nil {
		log.Error("Could not insert account access events", "error", err, "accounts_count", len(accounts))
		return fmt.Errorf("could not insert account access events: %w", err)
	}

	log.Debug("Inserted account access events", "count", len(accounts))
	return nil
}

// insertStorageAccessEventsInTx inserts ALL storage access events for archive mode
func (r *ClickHouseRepository) insertStorageAccessEventsInTx(ctx context.Context, tx *sql.Tx, storage map[string]map[string]uint64) error {
	if len(storage) == 0 {
		return nil
	}

	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse INSERT statement for storage_archive table
	query := `INSERT INTO storage_archive (address, slot_key, block_number) VALUES `

	var values []interface{}
	var placeholders []string

	for address, slots := range storage {
		// Convert address to binary format (FixedString(20))
		addressBytes, err := utils.HexToBytes(address)
		if err != nil {
			log.Warn("Skipping invalid address in storage", "address", address, "error", err)
			continue
		}

		for slot, blockNumber := range slots {
			// Convert slot to binary format (FixedString(32))
			slotBytes, err := utils.HexToBytes(slot)
			if err != nil {
				log.Warn("Skipping invalid slot", "address", address, "slot", slot, "error", err)
				continue
			}

			placeholders = append(placeholders, "(?, ?, ?)")
			values = append(values, addressBytes, slotBytes, blockNumber)
		}
	}

	if len(placeholders) == 0 {
		return nil // No valid storage slots to insert
	}

	fullQuery := query + strings.Join(placeholders, ", ")

	_, err := tx.ExecContext(ctx, fullQuery, values...)
	if err != nil {
		log.Error("Could not insert storage access events",
			"error", err,
			"storage_accounts", len(storage),
			"total_slots", len(placeholders))
		return fmt.Errorf("could not insert storage access events: %w", err)
	}

	log.Debug("Inserted storage access events", "total_slots", len(placeholders))
	return nil
}

// updateLastIndexedRangeInTx updates the last indexed range in metadata
func (r *ClickHouseRepository) updateLastIndexedRangeInTx(ctx context.Context, tx *sql.Tx, rangeNumber uint64) error {
	// ClickHouse uses ReplacingMergeTree, so we can simply INSERT the new value
	query := `INSERT INTO metadata_archive (key, value) VALUES (?, ?)`

	_, err := tx.ExecContext(ctx, query, "last_indexed_range", fmt.Sprintf("%d", rangeNumber))
	if err != nil {
		return fmt.Errorf("could not update last indexed range: %w", err)
	}

	return nil
}

// countStorageSlots counts total storage slots across all addresses for logging
func (r *ClickHouseRepository) countStorageSlots(storage map[string]map[string]uint64) int {
	total := 0
	for _, slots := range storage {
		total += len(slots)
	}
	return total
}

// API query methods (used by API server)
func (r *ClickHouseRepository) GetStateLastAccessedBlock(ctx context.Context, address string, slot *string) (uint64, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetStateLastAccessedBlock not yet implemented",
		"address", address,
		"slot", slot)
	return 0, fmt.Errorf("ClickHouse GetStateLastAccessedBlock not yet implemented")
}

func (r *ClickHouseRepository) GetAccountInfo(ctx context.Context, address string) (*Account, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetAccountInfo not yet implemented", "address", address)
	return nil, fmt.Errorf("ClickHouse GetAccountInfo not yet implemented")
}

func (r *ClickHouseRepository) GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetSyncStatus not yet implemented",
		"latest_range", latestRange,
		"range_size", rangeSize)
	return nil, fmt.Errorf("ClickHouse GetSyncStatus not yet implemented")
}

func (r *ClickHouseRepository) GetAnalyticsData(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*AnalyticsData, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetAnalyticsData not yet implemented",
		"expiry_block", expiryBlock,
		"current_block", currentBlock)
	return nil, fmt.Errorf("ClickHouse GetAnalyticsData not yet implemented")
}

// Additional query methods (for completeness)
func (r *ClickHouseRepository) GetExpiredStateCount(ctx context.Context, expiryBlock uint64) (int, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetExpiredStateCount not yet implemented", "expiry_block", expiryBlock)
	return 0, fmt.Errorf("ClickHouse GetExpiredStateCount not yet implemented")
}

func (r *ClickHouseRepository) GetTopNExpiredContracts(ctx context.Context, expiryBlock uint64, n int) ([]Contract, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetTopNExpiredContracts not yet implemented",
		"expiry_block", expiryBlock,
		"n", n)
	return nil, fmt.Errorf("ClickHouse GetTopNExpiredContracts not yet implemented")
}

func (r *ClickHouseRepository) GetAccountType(ctx context.Context, address string) (*bool, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetAccountType not yet implemented", "address", address)
	return nil, fmt.Errorf("ClickHouse GetAccountType not yet implemented")
}

func (r *ClickHouseRepository) GetExpiredAccountsByType(ctx context.Context, expiryBlock uint64, isContract *bool) ([]Account, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Error("ClickHouse GetExpiredAccountsByType not yet implemented",
		"expiry_block", expiryBlock,
		"is_contract", isContract)
	return nil, fmt.Errorf("ClickHouse GetExpiredAccountsByType not yet implemented")
}
