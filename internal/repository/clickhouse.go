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

	// Convert address to binary format
	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		log.Error("Invalid address format", "address", address, "error", err)
		return 0, fmt.Errorf("invalid address format: %w", err)
	}

	if slot == nil {
		// Query account access using latest_account_access view
		query := `SELECT last_access_block FROM latest_account_access WHERE address = ?`

		var lastAccessBlock uint64
		err := r.db.QueryRowContext(ctx, query, addressBytes).Scan(&lastAccessBlock)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Debug("Account not found", "address", address)
				return 0, fmt.Errorf("account not found")
			}
			log.Error("Could not query account last access", "address", address, "error", err)
			return 0, fmt.Errorf("could not query account last access: %w", err)
		}

		log.Debug("Retrieved account last access", "address", address, "block", lastAccessBlock)
		return lastAccessBlock, nil
	} else {
		// Query storage access using latest_storage_access view
		slotBytes, err := utils.HexToBytes(*slot)
		if err != nil {
			log.Error("Invalid slot format", "slot", *slot, "error", err)
			return 0, fmt.Errorf("invalid slot format: %w", err)
		}

		query := `SELECT last_access_block FROM latest_storage_access WHERE address = ? AND slot_key = ?`

		var lastAccessBlock uint64
		err = r.db.QueryRowContext(ctx, query, addressBytes, slotBytes).Scan(&lastAccessBlock)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Debug("Storage slot not found", "address", address, "slot", *slot)
				return 0, fmt.Errorf("storage slot not found")
			}
			log.Error("Could not query storage last access", "address", address, "slot", *slot, "error", err)
			return 0, fmt.Errorf("could not query storage last access: %w", err)
		}

		log.Debug("Retrieved storage last access", "address", address, "slot", *slot, "block", lastAccessBlock)
		return lastAccessBlock, nil
	}
}

func (r *ClickHouseRepository) GetAccountInfo(ctx context.Context, address string) (*Account, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Convert address to binary format
	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		log.Error("Invalid address format", "address", address, "error", err)
		return nil, fmt.Errorf("invalid address format: %w", err)
	}

	// Query using latest_account_access view
	query := `SELECT last_access_block, is_contract FROM latest_account_access WHERE address = ?`

	var lastAccessBlock uint64
	var isContractByte uint8
	err = r.db.QueryRowContext(ctx, query, addressBytes).Scan(&lastAccessBlock, &isContractByte)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Debug("Account not found", "address", address)
			return nil, fmt.Errorf("account not found")
		}
		log.Error("Could not query account info", "address", address, "error", err)
		return nil, fmt.Errorf("could not query account info: %w", err)
	}

	// Convert UInt8 back to bool
	isContract := isContractByte == 1

	account := &Account{
		Address:         address,
		LastAccessBlock: lastAccessBlock,
		IsContract:      &isContract,
	}

	log.Debug("Retrieved account info", "address", address, "last_access", lastAccessBlock, "is_contract", isContract)
	return account, nil
}

func (r *ClickHouseRepository) GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error) {
	log := logger.GetLogger("clickhouse-repo")

	lastIndexedRange, err := r.GetLastIndexedRange(ctx)
	if err != nil {
		log.Error("Could not get last indexed range for sync status", "error", err)
		return nil, fmt.Errorf("could not get last indexed range: %w", err)
	}

	isSynced := lastIndexedRange >= latestRange
	endBlock := (lastIndexedRange + 1) * rangeSize

	syncStatus := &SyncStatus{
		IsSynced:         isSynced,
		LastIndexedRange: lastIndexedRange,
		EndBlock:         endBlock,
	}

	log.Debug("Retrieved sync status",
		"is_synced", isSynced,
		"last_indexed_range", lastIndexedRange,
		"latest_range", latestRange,
		"end_block", endBlock)

	return syncStatus, nil
}

func (r *ClickHouseRepository) GetAnalyticsData(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*AnalyticsData, error) {
	log := logger.GetLogger("clickhouse-repo")
	log.Info("Starting ClickHouse analytics data retrieval", "expiry_block", expiryBlock, "current_block", currentBlock)

	analytics := &AnalyticsData{}

	// Get base statistics with a single optimized query
	baseStats, err := r.getBaseStatistics(ctx, expiryBlock)
	if err != nil {
		log.Error("Failed to get base statistics", "error", err)
		return nil, fmt.Errorf("failed to get base statistics: %w", err)
	}

	// Derive all analytics from base statistics (much more efficient)
	analytics.AccountExpiry = r.deriveAccountExpiryAnalysis(baseStats)
	analytics.AccountDistribution = r.deriveAccountDistributionAnalysis(baseStats)
	analytics.StorageSlotExpiry = r.deriveStorageSlotExpiryAnalysis(baseStats)

	// Get contract storage analysis (still needs separate query for top 10)
	if err := r.getContractStorageAnalysis(ctx, expiryBlock, &analytics.ContractStorage); err != nil {
		log.Error("Failed to get contract storage analysis", "error", err)
		return nil, fmt.Errorf("failed to get contract storage analysis: %w", err)
	}

	// Get storage expiry analysis and fully expired contracts (combined for efficiency)
	if err := r.getStorageExpiryAnalysis(ctx, expiryBlock, &analytics.StorageExpiry, &analytics.FullyExpiredContracts); err != nil {
		log.Error("Failed to get storage expiry analysis", "error", err)
		return nil, fmt.Errorf("failed to get storage expiry analysis: %w", err)
	}

	// Temporarily skip active contracts with expired storage analysis (same as PostgreSQL)
	// Set default empty values to avoid nil in response
	analytics.ActiveContractsExpiredStorage = ActiveContractsExpiredStorageAnalysis{
		ThresholdAnalysis:    []ExpiredStorageThreshold{},
		TotalActiveContracts: 0,
	}

	// Get complete expiry analysis
	if err := r.getCompleteExpiryAnalysis(ctx, expiryBlock, &analytics.CompleteExpiry); err != nil {
		log.Error("Failed to get complete expiry analysis", "error", err)
		return nil, fmt.Errorf("failed to get complete expiry analysis: %w", err)
	}

	log.Info("Successfully completed ClickHouse analytics data retrieval",
		"expired_accounts", analytics.AccountExpiry.TotalExpiredAccounts,
		"total_accounts", analytics.AccountExpiry.TotalAccounts,
		"expired_slots", analytics.StorageSlotExpiry.ExpiredSlots,
		"total_slots", analytics.StorageSlotExpiry.TotalSlots)

	return analytics, nil
}

// Additional query methods (for completeness)
func (r *ClickHouseRepository) GetExpiredStateCount(ctx context.Context, expiryBlock uint64) (int, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Count expired accounts using latest_account_access view
	query := `SELECT COUNT(*) FROM latest_account_access WHERE last_access_block < ?`

	var count int
	err := r.db.QueryRowContext(ctx, query, expiryBlock).Scan(&count)
	if err != nil {
		log.Error("Could not get expired state count", "expiry_block", expiryBlock, "error", err)
		return 0, fmt.Errorf("could not get expired state count: %w", err)
	}

	log.Debug("Retrieved expired state count", "count", count, "expiry_block", expiryBlock)
	return count, nil
}

func (r *ClickHouseRepository) GetTopNExpiredContracts(ctx context.Context, expiryBlock uint64, n int) ([]Contract, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Query using latest_storage_access view to find contracts with most expired slots
	query := `
		SELECT 
			lower(hex(s.address)) as address,
			COUNT(*) as slot_count
		FROM latest_storage_access s
		WHERE s.last_access_block < ?
		GROUP BY s.address
		ORDER BY slot_count DESC
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, expiryBlock, n)
	if err != nil {
		log.Error("Could not query top expired contracts", "expiry_block", expiryBlock, "n", n, "error", err)
		return nil, fmt.Errorf("could not query top expired contracts: %w", err)
	}
	defer rows.Close()

	var contracts []Contract
	for rows.Next() {
		var contract Contract
		err := rows.Scan(&contract.Address, &contract.SlotCount)
		if err != nil {
			log.Error("Could not scan contract row", "error", err)
			return nil, fmt.Errorf("could not scan contract row: %w", err)
		}

		// Add 0x prefix to address
		contract.Address = "0x" + contract.Address
		contracts = append(contracts, contract)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating contract rows", "error", err)
		return nil, fmt.Errorf("error iterating contract rows: %w", err)
	}

	log.Debug("Retrieved top expired contracts", "count", len(contracts), "expiry_block", expiryBlock)
	return contracts, nil
}

func (r *ClickHouseRepository) GetAccountType(ctx context.Context, address string) (*bool, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Convert address to binary format
	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		log.Error("Invalid address format", "address", address, "error", err)
		return nil, fmt.Errorf("invalid address format: %w", err)
	}

	// Query using latest_account_access view
	query := `SELECT is_contract FROM latest_account_access WHERE address = ?`

	var isContractByte uint8
	err = r.db.QueryRowContext(ctx, query, addressBytes).Scan(&isContractByte)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Debug("Account not found", "address", address)
			return nil, fmt.Errorf("account not found")
		}
		log.Error("Could not query account type", "address", address, "error", err)
		return nil, fmt.Errorf("could not query account type: %w", err)
	}

	// Convert UInt8 back to bool
	isContract := isContractByte == 1

	log.Debug("Retrieved account type", "address", address, "is_contract", isContract)
	return &isContract, nil
}

func (r *ClickHouseRepository) GetExpiredAccountsByType(ctx context.Context, expiryBlock uint64, isContract *bool) ([]Account, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Build query with optional contract type filter
	query := `SELECT lower(hex(address)), last_access_block, is_contract FROM latest_account_access WHERE last_access_block < ?`
	args := []interface{}{expiryBlock}

	if isContract != nil {
		query += ` AND is_contract = ?`
		var isContractByte uint8
		if *isContract {
			isContractByte = 1
		} else {
			isContractByte = 0
		}
		args = append(args, isContractByte)
	}

	query += ` ORDER BY last_access_block ASC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Error("Could not query expired accounts by type", "expiry_block", expiryBlock, "is_contract", isContract, "error", err)
		return nil, fmt.Errorf("could not query expired accounts by type: %w", err)
	}
	defer rows.Close()

	var accounts []Account
	for rows.Next() {
		var account Account
		var addressHex string
		var isContractByte uint8

		err := rows.Scan(&addressHex, &account.LastAccessBlock, &isContractByte)
		if err != nil {
			log.Error("Could not scan account row", "error", err)
			return nil, fmt.Errorf("could not scan account row: %w", err)
		}

		// Add 0x prefix to address and convert is_contract
		account.Address = "0x" + addressHex
		isContractBool := isContractByte == 1
		account.IsContract = &isContractBool

		accounts = append(accounts, account)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating account rows", "error", err)
		return nil, fmt.Errorf("error iterating account rows: %w", err)
	}

	log.Debug("Retrieved expired accounts by type", "count", len(accounts), "expiry_block", expiryBlock, "is_contract", isContract)
	return accounts, nil
}

// ClickHouse-specific analytics helper methods

// deriveAccountExpiryAnalysis derives account expiry analysis from base statistics
func (r *ClickHouseRepository) deriveAccountExpiryAnalysis(stats *BaseStatistics) AccountExpiryAnalysis {
	result := AccountExpiryAnalysis{
		ExpiredEOAs:          stats.ExpiredEOAs,
		ExpiredContracts:     stats.ExpiredContracts,
		TotalExpiredAccounts: stats.ExpiredAccounts(),
		TotalEOAs:            stats.TotalEOAs,
		TotalContracts:       stats.TotalContracts,
		TotalAccounts:        stats.TotalAccounts(),
	}

	// Calculate percentages
	if stats.TotalEOAs > 0 {
		result.ExpiredEOAPercentage = float64(stats.ExpiredEOAs) / float64(stats.TotalEOAs) * 100
	}
	if stats.TotalContracts > 0 {
		result.ExpiredContractPercentage = float64(stats.ExpiredContracts) / float64(stats.TotalContracts) * 100
	}
	if stats.TotalAccounts() > 0 {
		result.TotalExpiredPercentage = float64(stats.ExpiredAccounts()) / float64(stats.TotalAccounts()) * 100
	}

	return result
}

// deriveAccountDistributionAnalysis derives account distribution analysis from base statistics
func (r *ClickHouseRepository) deriveAccountDistributionAnalysis(stats *BaseStatistics) AccountDistributionAnalysis {
	result := AccountDistributionAnalysis{
		TotalExpiredAccounts: stats.ExpiredAccounts(),
	}

	// Calculate percentages among expired accounts
	if stats.ExpiredAccounts() > 0 {
		result.ContractPercentage = float64(stats.ExpiredContracts) / float64(stats.ExpiredAccounts()) * 100
		result.EOAPercentage = float64(stats.ExpiredEOAs) / float64(stats.ExpiredAccounts()) * 100
	}

	return result
}

// deriveStorageSlotExpiryAnalysis derives storage slot expiry analysis from base statistics
func (r *ClickHouseRepository) deriveStorageSlotExpiryAnalysis(stats *BaseStatistics) StorageSlotExpiryAnalysis {
	result := StorageSlotExpiryAnalysis{
		ExpiredSlots: stats.ExpiredSlots,
		TotalSlots:   stats.TotalSlots,
	}

	// Calculate percentage of expired slots
	if stats.TotalSlots > 0 {
		result.ExpiredSlotPercentage = float64(stats.ExpiredSlots) / float64(stats.TotalSlots) * 100
	}

	return result
}

// getContractStorageAnalysis gets the top 10 contracts with the largest expired state footprint
func (r *ClickHouseRepository) getContractStorageAnalysis(ctx context.Context, expiryBlock uint64, result *ContractStorageAnalysis) error {
	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse query using latest_storage_access view
	query := `
		WITH contract_storage_stats AS (
			SELECT 
				s.address,
				countIf(s.last_access_block < ?) as expired_slots,
				COUNT(*) as total_slots
			FROM latest_storage_access s
			GROUP BY s.address
			HAVING countIf(s.last_access_block < ?) > 0
		)
		SELECT 
			lower(hex(address)),
			expired_slots,
			total_slots,
			(expired_slots / total_slots * 100) as expiry_percentage
		FROM contract_storage_stats
		ORDER BY expired_slots DESC, expiry_percentage DESC
		LIMIT 10
	`

	rows, err := r.db.QueryContext(ctx, query, expiryBlock, expiryBlock)
	if err != nil {
		log.Error("Could not query contract storage analysis", "expiry_block", expiryBlock, "error", err)
		return fmt.Errorf("could not query contract storage analysis: %w", err)
	}
	defer rows.Close()

	var contracts []ExpiredContract
	for rows.Next() {
		var contract ExpiredContract
		var addressHex string

		err := rows.Scan(
			&addressHex,
			&contract.ExpiredSlotCount,
			&contract.TotalSlotCount,
			&contract.ExpiryPercentage,
		)
		if err != nil {
			log.Error("Could not scan expired contract row", "error", err)
			return fmt.Errorf("could not scan expired contract row: %w", err)
		}

		// Add 0x prefix to address
		contract.Address = "0x" + addressHex
		contracts = append(contracts, contract)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating expired contract rows", "error", err)
		return fmt.Errorf("error iterating expired contract rows: %w", err)
	}

	result.TopExpiredContracts = contracts
	log.Debug("Retrieved contract storage analysis", "count", len(contracts), "expiry_block", expiryBlock)
	return nil
}

// getStorageExpiryAnalysis gets storage expiry analysis and fully expired contracts (combined for efficiency)
func (r *ClickHouseRepository) getStorageExpiryAnalysis(ctx context.Context, expiryBlock uint64, storageResult *StorageExpiryAnalysis, fullyExpiredResult *FullyExpiredContractsAnalysis) error {
	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse query for storage expiry percentages per contract
	query := `
		WITH contract_expiry_stats AS (
			SELECT 
				address,
				countIf(last_access_block < ?) as expired_slots,
				COUNT(*) as total_slots,
				(countIf(last_access_block < ?) / COUNT(*) * 100) as expiry_percentage
			FROM latest_storage_access
			GROUP BY address
			HAVING COUNT(*) > 0
		)
		SELECT 
			AVG(expiry_percentage) as avg_expiry,
			quantile(0.5)(expiry_percentage) as median_expiry,
			COUNT(*) as contracts_analyzed,
			countIf(expiry_percentage = 100) as fully_expired_count
		FROM contract_expiry_stats
	`

	var avgExpiry, medianExpiry float64
	var contractsAnalyzed, fullyExpiredCount int

	err := r.db.QueryRowContext(ctx, query, expiryBlock, expiryBlock).Scan(
		&avgExpiry,
		&medianExpiry,
		&contractsAnalyzed,
		&fullyExpiredCount,
	)
	if err != nil {
		log.Error("Could not get storage expiry analysis", "expiry_block", expiryBlock, "error", err)
		return fmt.Errorf("could not get storage expiry analysis: %w", err)
	}

	// Set storage expiry analysis results
	storageResult.AverageExpiryPercentage = avgExpiry
	storageResult.MedianExpiryPercentage = medianExpiry
	storageResult.ContractsAnalyzed = contractsAnalyzed

	// Get expiry distribution buckets
	expiryDistribution, err := r.getExpiryDistributionBuckets(ctx, expiryBlock)
	if err != nil {
		log.Error("Could not get expiry distribution buckets", "error", err)
		return fmt.Errorf("could not get expiry distribution buckets: %w", err)
	}
	storageResult.ExpiryDistribution = expiryDistribution

	// Set fully expired contracts analysis results
	fullyExpiredResult.FullyExpiredContractCount = fullyExpiredCount
	fullyExpiredResult.TotalContractsWithStorage = contractsAnalyzed
	if contractsAnalyzed > 0 {
		fullyExpiredResult.FullyExpiredPercentage = float64(fullyExpiredCount) / float64(contractsAnalyzed) * 100
	}

	log.Debug("Retrieved storage expiry analysis",
		"avg_expiry", avgExpiry,
		"median_expiry", medianExpiry,
		"contracts_analyzed", contractsAnalyzed,
		"fully_expired", fullyExpiredCount)

	return nil
}

// getExpiryDistributionBuckets gets the distribution of contracts by expiry percentage ranges
func (r *ClickHouseRepository) getExpiryDistributionBuckets(ctx context.Context, expiryBlock uint64) ([]ExpiryPercentageBucket, error) {
	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse query for expiry distribution buckets
	query := `
		WITH contract_expiry_stats AS (
			SELECT 
				address,
				(countIf(last_access_block < ?) / COUNT(*) * 100) as expiry_percentage
			FROM latest_storage_access
			GROUP BY address
			HAVING COUNT(*) > 0
		),
		bucketed_stats AS (
			SELECT 
				CASE 
					WHEN expiry_percentage = 0 THEN 0
					WHEN expiry_percentage > 0 AND expiry_percentage <= 20 THEN 1
					WHEN expiry_percentage > 20 AND expiry_percentage <= 50 THEN 21
					WHEN expiry_percentage > 50 AND expiry_percentage <= 80 THEN 51
					WHEN expiry_percentage > 80 AND expiry_percentage < 100 THEN 81
					WHEN expiry_percentage = 100 THEN 100
					ELSE -1
				END as bucket_start,
				CASE 
					WHEN expiry_percentage = 0 THEN 0
					WHEN expiry_percentage > 0 AND expiry_percentage <= 20 THEN 20
					WHEN expiry_percentage > 20 AND expiry_percentage <= 50 THEN 50
					WHEN expiry_percentage > 50 AND expiry_percentage <= 80 THEN 80
					WHEN expiry_percentage > 80 AND expiry_percentage < 100 THEN 99
					WHEN expiry_percentage = 100 THEN 100
					ELSE -1
				END as bucket_end
			FROM contract_expiry_stats
		)
		SELECT 
			bucket_start,
			bucket_end,
			COUNT(*) as count
		FROM bucketed_stats
		WHERE bucket_start >= 0
		GROUP BY bucket_start, bucket_end
		ORDER BY bucket_start
	`

	rows, err := r.db.QueryContext(ctx, query, expiryBlock)
	if err != nil {
		log.Error("Could not query expiry distribution buckets", "expiry_block", expiryBlock, "error", err)
		return nil, fmt.Errorf("could not query expiry distribution buckets: %w", err)
	}
	defer rows.Close()

	var buckets []ExpiryPercentageBucket
	for rows.Next() {
		var bucket ExpiryPercentageBucket
		err := rows.Scan(&bucket.RangeStart, &bucket.RangeEnd, &bucket.Count)
		if err != nil {
			log.Error("Could not scan bucket row", "error", err)
			return nil, fmt.Errorf("could not scan bucket row: %w", err)
		}
		buckets = append(buckets, bucket)
	}

	if err := rows.Err(); err != nil {
		log.Error("Error iterating bucket rows", "error", err)
		return nil, fmt.Errorf("error iterating bucket rows: %w", err)
	}

	log.Debug("Retrieved expiry distribution buckets", "count", len(buckets), "expiry_block", expiryBlock)
	return buckets, nil
}

// getCompleteExpiryAnalysis gets contracts that are fully expired at both account and storage levels
func (r *ClickHouseRepository) getCompleteExpiryAnalysis(ctx context.Context, expiryBlock uint64, result *CompleteExpiryAnalysis) error {
	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse query for complete expiry analysis
	query := `
		WITH contract_addresses AS (
			SELECT DISTINCT address
			FROM latest_storage_access
		),
		contract_account_expiry AS (
			SELECT 
				ca.address,
				aa.last_access_block < ? as account_expired
			FROM contract_addresses ca
			LEFT JOIN latest_account_access aa ON ca.address = aa.address
		),
		contract_storage_expiry AS (
			SELECT 
				address,
				countIf(last_access_block < ?) = COUNT(*) as all_storage_expired
			FROM latest_storage_access
			GROUP BY address
		)
		SELECT 
			COUNT(*) as total_contracts_with_storage,
			countIf(cae.account_expired AND cse.all_storage_expired) as fully_expired_count
		FROM contract_account_expiry cae
		JOIN contract_storage_expiry cse ON cae.address = cse.address
	`

	var totalContracts, fullyExpiredCount int
	err := r.db.QueryRowContext(ctx, query, expiryBlock, expiryBlock).Scan(&totalContracts, &fullyExpiredCount)
	if err != nil {
		log.Error("Could not get complete expiry analysis", "expiry_block", expiryBlock, "error", err)
		return fmt.Errorf("could not get complete expiry analysis: %w", err)
	}

	result.FullyExpiredContractCount = fullyExpiredCount
	result.TotalContractsWithStorage = totalContracts
	if totalContracts > 0 {
		result.FullyExpiredPercentage = float64(fullyExpiredCount) / float64(totalContracts) * 100
	}

	log.Debug("Retrieved complete expiry analysis",
		"total_contracts", totalContracts,
		"fully_expired", fullyExpiredCount,
		"percentage", result.FullyExpiredPercentage)

	return nil
}

// getBaseStatistics retrieves all basic statistics in a single optimized ClickHouse query
func (r *ClickHouseRepository) getBaseStatistics(ctx context.Context, expiryBlock uint64) (*BaseStatistics, error) {
	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse query using latest_account_access and latest_storage_access views
	query := `
		WITH account_stats AS (
			SELECT 
				countIf(is_contract = 0) as total_eoas,
				countIf(is_contract = 1) as total_contracts,
				countIf(last_access_block < ? AND is_contract = 0) as expired_eoas,
				countIf(last_access_block < ? AND is_contract = 1) as expired_contracts
			FROM latest_account_access
		),
		storage_stats AS (
			SELECT 
				COUNT(*) as total_slots,
				countIf(last_access_block < ?) as expired_slots
			FROM latest_storage_access
		)
		SELECT 
			a.total_eoas,
			a.total_contracts,
			a.expired_eoas,
			a.expired_contracts,
			s.total_slots,
			s.expired_slots
		FROM account_stats a, storage_stats s
	`

	var stats BaseStatistics
	err := r.db.QueryRowContext(ctx, query, expiryBlock, expiryBlock, expiryBlock).Scan(
		&stats.TotalEOAs,
		&stats.TotalContracts,
		&stats.ExpiredEOAs,
		&stats.ExpiredContracts,
		&stats.TotalSlots,
		&stats.ExpiredSlots,
	)
	if err != nil {
		log.Error("Could not get base statistics", "expiry_block", expiryBlock, "error", err)
		return nil, fmt.Errorf("could not get base statistics: %w", err)
	}

	log.Debug("Retrieved base statistics",
		"total_eoas", stats.TotalEOAs,
		"total_contracts", stats.TotalContracts,
		"expired_eoas", stats.ExpiredEOAs,
		"expired_contracts", stats.ExpiredContracts,
		"total_slots", stats.TotalSlots,
		"expired_slots", stats.ExpiredSlots)

	return &stats, nil
}
