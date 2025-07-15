package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/weiihann/state-expiry-indexer/internal/logger"
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

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("Could not begin transaction", "error", err)
		return 0, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	var value string
	// Use argMax to get the most recent value based on updated_at timestamp
	// This ensures we get the latest value even before background merges occur
	query := "SELECT argMax(value, updated_at) FROM metadata_archive WHERE key = 'last_indexed_range'"
	err = tx.QueryRowContext(ctx, query).Scan(&value)
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

func (r *ClickHouseRepository) GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error) {
	log := logger.GetLogger("clickhouse-repo")

	lastIndexedRange, err := r.GetLastIndexedRange(ctx)
	if err != nil {
		log.Error("Could not get last indexed range for sync status", "error", err)
		return nil, fmt.Errorf("could not get last indexed range: %w", err)
	}

	// Calculate the end block of the last indexed range
	var endBlock uint64
	if lastIndexedRange == 0 {
		endBlock = 0 // Genesis range
	} else {
		endBlock = lastIndexedRange * rangeSize
	}

	isSynced := lastIndexedRange >= latestRange

	log.Debug("Retrieved sync status",
		"is_synced", isSynced,
		"last_indexed_range", lastIndexedRange,
		"latest_range", latestRange,
		"end_block", endBlock)

	return &SyncStatus{
		IsSynced:         isSynced,
		LastIndexedRange: lastIndexedRange,
		EndBlock:         endBlock,
	}, nil
}

// InsertRange processes all events for archive mode (stores ALL events, not just latest)
func (r *ClickHouseRepository) InsertRange(
	ctx context.Context,
	accountAccesses map[uint64]map[string]struct{},
	accountType map[string]bool,
	storageAccesses map[uint64]map[string]map[string]struct{},
	rangeNumber uint64,
) error {
	log := logger.GetLogger("clickhouse-repo")

	log.Info("Inserting range", "range_number", rangeNumber)

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("Could not begin transaction", "error", err)
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert all account access events
	if err := r.insertAllAccountAccessEventsInTx(ctx, tx, accountAccesses, accountType); err != nil {
		log.Error("Could not insert all account access events", "error", err)
		return fmt.Errorf("could not insert all account access events: %w", err)
	}

	// Insert all storage access events
	if err := r.insertAllStorageAccessEventsInTx(ctx, tx, storageAccesses); err != nil {
		log.Error("Could not insert all storage access events", "error", err)
		return fmt.Errorf("could not insert all storage access events: %w", err)
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

	log.Info("Successfully inserted range", "range", rangeNumber)

	return nil
}

// insertAllAccountAccessEventsInTx inserts ALL account access events for archive mode
func (r *ClickHouseRepository) insertAllAccountAccessEventsInTx(
	ctx context.Context,
	tx *sql.Tx,
	accountAccesses map[uint64]map[string]struct{},
	accountType map[string]bool,
) error {
	if len(accountAccesses) == 0 {
		return nil
	}

	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse INSERT statement for accounts_archive table
	query := `INSERT INTO accounts_archive (address, block_number, is_contract) VALUES `

	var values []interface{}
	var placeholders []string

	// Sort the account accesses by block number
	blockNumbers := make([]uint64, 0, len(accountAccesses))
	for blockNumber := range accountAccesses {
		blockNumbers = append(blockNumbers, blockNumber)
	}
	sort.Slice(blockNumbers, func(i, j int) bool {
		return blockNumbers[i] < blockNumbers[j]
	})

	for _, blockNumber := range blockNumbers {
		for addr := range accountAccesses[blockNumber] {
			// Clean the address hex string (remove 0x prefix and ensure proper length)
			addressHex := strings.TrimPrefix(addr, "0x")

			if len(addressHex) != 40 {
				log.Error("Invalid address length", "address", addr, "hex_length", len(addressHex))
				return fmt.Errorf("invalid address length: %s", addr)
			}

			placeholders = append(placeholders, "(unhex(?), ?, ?)")
			values = append(values, addressHex, blockNumber, func() uint8 {
				isContract := accountType[addr]
				if isContract {
					return 1
				}
				return 0
			}())
		}
	}

	if len(placeholders) == 0 {
		return nil // No valid account accesses to insert
	}

	fullQuery := query + strings.Join(placeholders, ", ")

	_, err := tx.ExecContext(ctx, fullQuery, values...)
	if err != nil {
		// Show first few values for debugging
		debugValues := values
		if len(values) > 6 {
			debugValues = values[:6]
		}
		log.Error("Could not insert all account access events",
			"error", err,
			"account_events", len(accountAccesses),
			"query", fullQuery,
			"values_count", len(values),
			"first_few_values", debugValues)
		return fmt.Errorf("could not insert all account access events: %w", err)
	}

	log.Debug("Inserted all account access events", "count", len(accountAccesses))
	return nil
}

// insertAllStorageAccessEventsInTx inserts ALL storage access events for archive mode
func (r *ClickHouseRepository) insertAllStorageAccessEventsInTx(ctx context.Context, tx *sql.Tx, storageAccesses map[uint64]map[string]map[string]struct{}) error {
	if len(storageAccesses) == 0 {
		return nil
	}

	log := logger.GetLogger("clickhouse-repo")

	// ClickHouse INSERT statement for storage_archive table
	query := `INSERT INTO storage_archive (address, slot_key, block_number) VALUES `

	var values []interface{}
	var placeholders []string

	// Sort the storage accesses by block number
	blockNumbers := make([]uint64, 0, len(storageAccesses))
	for blockNumber := range storageAccesses {
		blockNumbers = append(blockNumbers, blockNumber)
	}
	sort.Slice(blockNumbers, func(i, j int) bool {
		return blockNumbers[i] < blockNumbers[j]
	})

	for blockNumber, record := range storageAccesses {
		for addr, slot := range record {

			// Clean the address hex string (remove 0x prefix and ensure proper length)
			addressHex := strings.TrimPrefix(addr, "0x")
			if len(addressHex) != 40 {
				log.Error("Invalid address length", "address", addr, "hex_length", len(addressHex))
				return fmt.Errorf("invalid address length: %s", addr)
			}

			for slotKey := range slot {
				slotHex := strings.TrimPrefix(slotKey, "0x")
				if len(slotHex) != 64 {
					log.Error("Invalid slot length", "slot", slotKey, "hex_length", len(slotHex))
					return fmt.Errorf("invalid slot length: %s", slotKey)
				}

				placeholders = append(placeholders, "(unhex(?), unhex(?), ?)")
				values = append(values, addressHex, slotHex, blockNumber)
			}
		}
	}

	if len(placeholders) == 0 {
		return nil // No valid storage accesses to insert
	}

	fullQuery := query + strings.Join(placeholders, ", ")

	_, err := tx.ExecContext(ctx, fullQuery, values...)
	if err != nil {
		log.Error("Could not insert all storage access events",
			"error", err,
			"storage_events", len(storageAccesses),
			"total_events", len(placeholders))
		return fmt.Errorf("could not insert all storage access events: %w", err)
	}

	log.Debug("Inserted all storage access events", "count", len(storageAccesses))
	return nil
}

// ==============================================================================
// OPTIMIZED ANALYTICS METHODS (Questions 1-15)
// ==============================================================================

// GetAccountAnalytics - Questions 1, 2, 5a
func (r *ClickHouseRepository) GetAccountAnalytics(ctx context.Context, params QueryParams) (*AccountAnalytics, error) {
	log := logger.GetLogger("clickhouse-repo")
	startTime := time.Now()

	// Single optimized query using materialized views and aggregated tables
	query := `
	WITH
	  collapsed_accounts AS (
    	SELECT
      		address,
      		argMax(is_contract, last_access_block) AS is_contract,
      		max(last_access_block)                 AS max_access_block
    	FROM accounts_state
    	GROUP BY address
  	),
	account_stats AS (
		SELECT
		countIf(is_contract = 0)                                     AS total_eoas,
		countIf(is_contract = 1)                                     AS total_contracts,
		countIf(is_contract = 0 AND max_access_block < ?)         	 AS expired_eoas,
		countIf(is_contract = 1 AND max_access_block < ?)         	 AS expired_contracts
		FROM collapsed_accounts
	),

	access_counts AS (
		SELECT
		address,
		argMaxMerge(is_contract_state) AS is_contract,
		countMerge(access_count)       AS access_count
		FROM mv_account_access_count
		GROUP BY address
	),

	single_access_stats AS (
		SELECT
		countIf(is_contract = 0 AND access_count = 1)                AS single_access_eoas,
		countIf(is_contract = 1 AND access_count = 1)                AS single_access_contracts
		FROM access_counts
	)

	SELECT
	stats.total_eoas,
	stats.total_contracts,
	stats.expired_eoas,
	stats.expired_contracts,
	sas.single_access_eoas,
	sas.single_access_contracts
	FROM account_stats AS stats
	CROSS JOIN single_access_stats AS sas
	;
	`

	var totalEOAs, totalContracts, expiredEOAs, expiredContracts, singleAccessEOAs, singleAccessContracts int

	err := r.db.QueryRowContext(ctx, query, params.ExpiryBlock, params.ExpiryBlock).Scan(
		&totalEOAs, &totalContracts, &expiredEOAs, &expiredContracts,
		&singleAccessEOAs, &singleAccessContracts,
	)
	if err != nil {
		log.Error("Could not get account analytics", "error", err)
		return nil, fmt.Errorf("could not get account analytics: %w", err)
	}

	// Calculate derived values
	totalAccounts := totalEOAs + totalContracts
	totalExpired := expiredEOAs + expiredContracts
	totalSingleAccess := singleAccessEOAs + singleAccessContracts

	var expiryRate, singleAccessRate, eoaPercentage, contractPercentage float64
	if totalAccounts > 0 {
		expiryRate = float64(totalExpired) / float64(totalAccounts) * 100
		singleAccessRate = float64(totalSingleAccess) / float64(totalAccounts) * 100
		eoaPercentage = float64(totalEOAs) / float64(totalAccounts) * 100
		contractPercentage = float64(totalContracts) / float64(totalAccounts) * 100
	}

	result := &AccountAnalytics{
		Total: AccountTotals{
			EOAs:      totalEOAs,
			Contracts: totalContracts,
			Total:     totalAccounts,
		},
		Expiry: AccountExpiryData{
			ExpiredEOAs:      expiredEOAs,
			ExpiredContracts: expiredContracts,
			TotalExpired:     totalExpired,
			ExpiryRate:       expiryRate,
		},
		SingleAccess: AccountSingleAccessData{
			SingleAccessEOAs:      singleAccessEOAs,
			SingleAccessContracts: singleAccessContracts,
			TotalSingleAccess:     totalSingleAccess,
			SingleAccessRate:      singleAccessRate,
		},
		Distribution: AccountDistribution{
			EOAPercentage:      eoaPercentage,
			ContractPercentage: contractPercentage,
		},
	}

	log.Debug("Retrieved account analytics",
		"total_accounts", totalAccounts,
		"expired_accounts", totalExpired,
		"duration_ms", time.Since(startTime).Milliseconds())

	return result, nil
}

// GetStorageAnalytics - Questions 3, 4, 5b
func (r *ClickHouseRepository) GetStorageAnalytics(ctx context.Context, params QueryParams) (*StorageAnalytics, error) {
	log := logger.GetLogger("clickhouse-repo")
	startTime := time.Now()

	// Single optimized query using materialized views and aggregated tables
	query := `
	WITH 
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	),
	storage_stats AS (
		SELECT 
			COUNT(*) as total_slots,
			countIf(max_access_block < ?) as expired_slots
		FROM collapsed_storage
	),
	storage_access_counts AS (
		SELECT 
			address,
			slot_key,
			countMerge(access_count) as access_count
		FROM mv_storage_access_count
		GROUP BY address, slot_key
	),
	single_access_stats AS (
		SELECT 
			countIf(access_count = 1) as single_access_slots
		FROM storage_access_counts
	)
	SELECT 
		s_stats.total_slots,
		s_stats.expired_slots,
		sa_stats.single_access_slots
	FROM storage_stats s_stats
	CROSS JOIN single_access_stats sa_stats
	`

	var totalSlots, expiredSlots, singleAccessSlots int

	err := r.db.QueryRowContext(ctx, query, params.ExpiryBlock).Scan(
		&totalSlots, &expiredSlots, &singleAccessSlots,
	)
	if err != nil {
		log.Error("Could not get storage analytics", "error", err)
		return nil, fmt.Errorf("could not get storage analytics: %w", err)
	}

	// Calculate derived values
	activeSlots := totalSlots - expiredSlots
	var expiryRate, singleAccessRate float64
	if totalSlots > 0 {
		expiryRate = float64(expiredSlots) / float64(totalSlots) * 100
		singleAccessRate = float64(singleAccessSlots) / float64(totalSlots) * 100
	}

	result := &StorageAnalytics{
		Total: StorageTotals{
			TotalSlots: totalSlots,
		},
		Expiry: StorageExpiryData{
			ExpiredSlots: expiredSlots,
			ActiveSlots:  activeSlots,
			ExpiryRate:   expiryRate,
		},
		SingleAccess: StorageSingleAccessData{
			SingleAccessSlots: singleAccessSlots,
			SingleAccessRate:  singleAccessRate,
		},
	}

	log.Debug("Retrieved storage analytics",
		"total_slots", totalSlots,
		"expired_slots", expiredSlots,
		"duration_ms", time.Since(startTime).Milliseconds())

	return result, nil
}

// GetContractAnalytics - Questions 7, 8, 9, 10, 11, 15
func (r *ClickHouseRepository) GetContractAnalytics(ctx context.Context, params QueryParams) (*ContractAnalytics, error) {
	log := logger.GetLogger("clickhouse-repo")
	startTime := time.Now()

	// Get contract rankings
	rankings, err := r.getContractRankings(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get contract rankings: %w", err)
	}

	// Get contract expiry analysis
	expiryAnalysis, err := r.getContractExpiryAnalysis(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get contract expiry analysis: %w", err)
	}

	// Get contract volume analysis
	volumeAnalysis, err := r.getContractVolumeAnalysis(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get contract volume analysis: %w", err)
	}

	// Get contract status analysis
	statusAnalysis, err := r.getContractStatusAnalysis(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get contract status analysis: %w", err)
	}

	result := &ContractAnalytics{
		Rankings:       rankings,
		ExpiryAnalysis: expiryAnalysis,
		VolumeAnalysis: volumeAnalysis,
		StatusAnalysis: statusAnalysis,
	}

	log.Debug("Retrieved contract analytics",
		"rankings_count", len(rankings.TopByExpiredSlots),
		"duration_ms", time.Since(startTime).Milliseconds())

	return result, nil
}

// getContractRankings gets contract rankings for top expired and total slots
func (r *ClickHouseRepository) getContractRankings(ctx context.Context, params QueryParams) (ContractRankings, error) {
	// Get top contracts by expired slots
	topExpiredQuery := `
	WITH 
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	)
	SELECT 
		lower(hex(address)) as address,
		COUNT(*) as total_slots,
		countIf(max_access_block < ?) as expired_slots,
		countIf(max_access_block >= ?) as active_slots,
		(countIf(max_access_block < ?) / COUNT(*) * 100) as expiry_percentage,
		max(max_access_block) as last_access,
		any(max_access_block >= ?) as is_account_active
	FROM collapsed_storage
	GROUP BY address
	HAVING expired_slots > 0
	ORDER BY expired_slots DESC, expiry_percentage DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, topExpiredQuery,
		params.ExpiryBlock, params.ExpiryBlock, params.ExpiryBlock, params.ExpiryBlock, params.TopN)
	if err != nil {
		return ContractRankings{}, fmt.Errorf("could not query top expired contracts: %w", err)
	}
	defer rows.Close()

	var topByExpiredSlots []ContractRankingItem
	for rows.Next() {
		var item ContractRankingItem
		var addressHex string
		err := rows.Scan(&addressHex, &item.TotalSlots, &item.ExpiredSlots, &item.ActiveSlots,
			&item.ExpiryPercentage, &item.LastAccess, &item.IsAccountActive)
		if err != nil {
			return ContractRankings{}, fmt.Errorf("could not scan contract ranking row: %w", err)
		}
		item.Address = "0x" + addressHex
		topByExpiredSlots = append(topByExpiredSlots, item)
	}

	// Get top contracts by total slots
	topTotalQuery := `
	WITH 
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	)
	SELECT 
		lower(hex(address)) as address,
		COUNT(*) as total_slots,
		countIf(max_access_block < ?) as expired_slots,
		countIf(max_access_block >= ?) as active_slots,
		(countIf(max_access_block < ?) / COUNT(*) * 100) as expiry_percentage,
		max(max_access_block) as last_access,
		any(max_access_block >= ?) as is_account_active
	FROM collapsed_storage
	GROUP BY address
	ORDER BY total_slots DESC
	LIMIT ?
	`

	rows, err = r.db.QueryContext(ctx, topTotalQuery,
		params.ExpiryBlock, params.ExpiryBlock, params.ExpiryBlock, params.ExpiryBlock, params.TopN)
	if err != nil {
		return ContractRankings{}, fmt.Errorf("could not query top total contracts: %w", err)
	}
	defer rows.Close()

	var topByTotalSlots []ContractRankingItem
	for rows.Next() {
		var item ContractRankingItem
		var addressHex string
		err := rows.Scan(&addressHex, &item.TotalSlots, &item.ExpiredSlots, &item.ActiveSlots,
			&item.ExpiryPercentage, &item.LastAccess, &item.IsAccountActive)
		if err != nil {
			return ContractRankings{}, fmt.Errorf("could not scan contract ranking row: %w", err)
		}
		item.Address = "0x" + addressHex
		topByTotalSlots = append(topByTotalSlots, item)
	}

	return ContractRankings{
		TopByExpiredSlots: topByExpiredSlots,
		TopByTotalSlots:   topByTotalSlots,
	}, nil
}

// getContractExpiryAnalysis gets contract expiry distribution analysis
func (r *ClickHouseRepository) getContractExpiryAnalysis(ctx context.Context, params QueryParams) (ContractExpiryAnalysis, error) {
	query := `
	WITH
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	),
	contract_expiry_stats AS (
		SELECT 
			address,
			(countIf(max_access_block < ?) / COUNT(*) * 100) as expiry_percentage
		FROM collapsed_storage
		GROUP BY address
		HAVING COUNT(*) > 0
	)
	SELECT 
		avg(expiry_percentage) as avg_expiry,
		quantile(0.5)(expiry_percentage) as median_expiry,
		COUNT(*) as contracts_analyzed
	FROM contract_expiry_stats
	`

	var avgExpiry, medianExpiry float64
	var contractsAnalyzed int

	err := r.db.QueryRowContext(ctx, query, params.ExpiryBlock).Scan(
		&avgExpiry, &medianExpiry, &contractsAnalyzed,
	)
	if err != nil {
		return ContractExpiryAnalysis{}, fmt.Errorf("could not get contract expiry analysis: %w", err)
	}

	// Get expiry distribution buckets
	distribution, err := r.getExpiryDistributionBuckets(ctx, params)
	if err != nil {
		return ContractExpiryAnalysis{}, fmt.Errorf("could not get expiry distribution: %w", err)
	}

	return ContractExpiryAnalysis{
		AverageExpiryPercentage: avgExpiry,
		MedianExpiryPercentage:  medianExpiry,
		ExpiryDistribution:      distribution,
		ContractsAnalyzed:       contractsAnalyzed,
	}, nil
}

// getExpiryDistributionBuckets gets expiry distribution buckets
func (r *ClickHouseRepository) getExpiryDistributionBuckets(ctx context.Context, params QueryParams) ([]ExpiryDistributionBucket, error) {
	query := `
	WITH contract_expiry_stats AS (
		SELECT 
			address,
			(countIf(last_access_block <= ?) / COUNT(*) * 100) as expiry_percentage
		FROM storage_state
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
			END as bucket_start,
			CASE 
				WHEN expiry_percentage = 0 THEN 0
				WHEN expiry_percentage > 0 AND expiry_percentage <= 20 THEN 20
				WHEN expiry_percentage > 20 AND expiry_percentage <= 50 THEN 50
				WHEN expiry_percentage > 50 AND expiry_percentage <= 80 THEN 80
				WHEN expiry_percentage > 80 AND expiry_percentage < 100 THEN 99
				WHEN expiry_percentage = 100 THEN 100
			END as bucket_end
		FROM contract_expiry_stats
	)
	SELECT 
		bucket_start,
		bucket_end,
		COUNT(*) as count
	FROM bucketed_stats
	GROUP BY bucket_start, bucket_end
	ORDER BY bucket_start
	`

	rows, err := r.db.QueryContext(ctx, query, params.ExpiryBlock)
	if err != nil {
		return nil, fmt.Errorf("could not query expiry distribution: %w", err)
	}
	defer rows.Close()

	var buckets []ExpiryDistributionBucket
	for rows.Next() {
		var bucket ExpiryDistributionBucket
		err := rows.Scan(&bucket.RangeStart, &bucket.RangeEnd, &bucket.Count)
		if err != nil {
			return nil, fmt.Errorf("could not scan bucket row: %w", err)
		}
		buckets = append(buckets, bucket)
	}

	return buckets, nil
}

// getContractVolumeAnalysis gets contract volume analysis
func (r *ClickHouseRepository) getContractVolumeAnalysis(ctx context.Context) (ContractVolumeAnalysis, error) {
	query := `
	WITH 
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	),
	contract_storage_counts AS (
		SELECT 
			address,
			COUNT(*) as slot_count
		FROM collapsed_storage
		GROUP BY address
	)
	SELECT 
		avg(slot_count) as avg_storage,
		quantile(0.5)(slot_count) as median_storage,
		max(slot_count) as max_storage,
		min(slot_count) as min_storage,
		COUNT(*) as total_contracts
	FROM contract_storage_counts
	`

	var avgStorage, medianStorage float64
	var maxStorage, minStorage, totalContracts int

	err := r.db.QueryRowContext(ctx, query).Scan(
		&avgStorage, &medianStorage, &maxStorage, &minStorage, &totalContracts,
	)
	if err != nil {
		return ContractVolumeAnalysis{}, fmt.Errorf("could not get contract volume analysis: %w", err)
	}

	return ContractVolumeAnalysis{
		AverageStoragePerContract: avgStorage,
		MedianStoragePerContract:  medianStorage,
		MaxStoragePerContract:     maxStorage,
		MinStoragePerContract:     minStorage,
		TotalContracts:            totalContracts,
	}, nil
}

// getContractStatusAnalysis gets contract status analysis
func (r *ClickHouseRepository) getContractStatusAnalysis(ctx context.Context, params QueryParams) (ContractStatusAnalysis, error) {
	query := `
	WITH
	collapsed_accounts AS (
		SELECT
			address,
			argMax(is_contract, last_access_block) AS is_contract,
			max(last_access_block)                 AS max_access_block
		FROM accounts_state
		GROUP BY address
	),
	collapsed_storage AS (
		SELECT
			address,
			slot_key,
			max(last_access_block) as max_access_block
		FROM storage_state
		GROUP BY address, slot_key
	),
	contract_status AS (
		SELECT 
			s.address,
			COUNT(*) as total_slots,
			countIf(s.max_access_block < ?) as expired_slots,
			any(a.max_access_block >= ?) as account_active
		FROM collapsed_storage s
		JOIN collapsed_accounts a ON s.address = a.address
		GROUP BY s.address
	)
	SELECT 
		countIf(expired_slots = total_slots) as all_expired_contracts,
		countIf(expired_slots = 0) as all_active_contracts,
		countIf(expired_slots > 0 AND expired_slots < total_slots) as mixed_state_contracts,
		countIf(account_active = 1 AND expired_slots > 0) as active_with_expired_storage,
		COUNT(*) as total_contracts
	FROM contract_status
	`

	var allExpired, allActive, mixedState, activeWithExpired, totalContracts int

	err := r.db.QueryRowContext(ctx, query, params.ExpiryBlock, params.ExpiryBlock).Scan(
		&allExpired, &allActive, &mixedState, &activeWithExpired, &totalContracts,
	)
	if err != nil {
		return ContractStatusAnalysis{}, fmt.Errorf("could not get contract status analysis: %w", err)
	}

	var allExpiredRate, allActiveRate float64
	if totalContracts > 0 {
		allExpiredRate = float64(allExpired) / float64(totalContracts) * 100
		allActiveRate = float64(allActive) / float64(totalContracts) * 100
	}

	return ContractStatusAnalysis{
		AllExpiredContracts:      allExpired,
		AllActiveContracts:       allActive,
		MixedStateContracts:      mixedState,
		ActiveWithExpiredStorage: activeWithExpired,
		AllExpiredRate:           allExpiredRate,
		AllActiveRate:            allActiveRate,
	}, nil
}

// GetBlockActivityAnalytics - Questions 6, 12, 13, 14
func (r *ClickHouseRepository) GetBlockActivityAnalytics(ctx context.Context, params QueryParams) (*BlockActivityAnalytics, error) {
	log := logger.GetLogger("clickhouse-repo")
	startTime := time.Now()

	// Get top activity blocks
	topBlocks, err := r.GetTopActivityBlocks(ctx, params.StartBlock, params.EndBlock, params.TopN)
	if err != nil {
		return nil, fmt.Errorf("could not get top activity blocks: %w", err)
	}

	// Get time series data
	timeSeriesData, err := r.GetTimeSeriesData(ctx, params.StartBlock, params.EndBlock, params.WindowSize)
	if err != nil {
		return nil, fmt.Errorf("could not get time series data: %w", err)
	}

	// Get access rates
	accessRates, err := r.GetAccessRates(ctx, params.StartBlock, params.EndBlock)
	if err != nil {
		return nil, fmt.Errorf("could not get access rates: %w", err)
	}

	// Get frequency data
	frequencyData, err := r.getFrequencyAnalysis(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get frequency analysis: %w", err)
	}

	// Get trend data
	trendData, err := r.GetTrendAnalysis(ctx, params.StartBlock, params.EndBlock)
	if err != nil {
		return nil, fmt.Errorf("could not get trend analysis: %w", err)
	}

	result := &BlockActivityAnalytics{
		TopBlocks:      topBlocks,
		TimeSeriesData: timeSeriesData,
		AccessRates:    *accessRates,
		FrequencyData:  frequencyData,
		TrendData:      *trendData,
	}

	log.Debug("Retrieved block activity analytics",
		"top_blocks_count", len(topBlocks),
		"time_series_count", len(timeSeriesData),
		"duration_ms", time.Since(startTime).Milliseconds())

	return result, nil
}

// getFrequencyAnalysis gets frequency analysis data
func (r *ClickHouseRepository) getFrequencyAnalysis(ctx context.Context, params QueryParams) (FrequencyAnalysis, error) {
	// Get account frequency data
	accountFreq, err := r.getMostFrequentAccounts(ctx, params.TopN)
	if err != nil {
		return FrequencyAnalysis{}, fmt.Errorf("could not get account frequency: %w", err)
	}

	// Get storage frequency data
	storageFreq, err := r.getMostFrequentStorage(ctx, params.TopN)
	if err != nil {
		return FrequencyAnalysis{}, fmt.Errorf("could not get storage frequency: %w", err)
	}

	return FrequencyAnalysis{
		AccountFrequency: accountFreq,
		StorageFrequency: storageFreq,
	}, nil
}

// getMostFrequentAccounts gets most frequently accessed accounts
func (r *ClickHouseRepository) getMostFrequentAccounts(ctx context.Context, topN int) (AccountFrequencyData, error) {
	query := `
	WITH access_counts AS (
		SELECT
			address,
			argMaxMerge(is_contract_state) AS is_contract,  -- UInt8 flag
			countMerge(access_count)       AS access_count  -- UInt64
		FROM mv_account_access_count
		GROUP BY address
	)
	SELECT
		avg(access_count)           AS avg_frequency,
		quantile(0.5)(access_count) AS median_frequency
	FROM access_counts
	`

	var avgFreq, medianFreq float64
	err := r.db.QueryRowContext(ctx, query).Scan(&avgFreq, &medianFreq)
	if err != nil {
		return AccountFrequencyData{}, fmt.Errorf("could not get account frequency stats: %w", err)
	}

	// Get most frequent accounts
	frequentQuery := `
	WITH access_counts AS (
		SELECT
			address,
			argMaxMerge(is_contract_state) AS is_contract,  -- UInt8 flag
			countMerge(access_count)       AS access_count  -- UInt64
		FROM mv_account_access_count
		GROUP BY address
	)

	SELECT 
		lower(hex(address)) as address,
		access_count,
		is_contract
	FROM access_counts
	ORDER BY access_count DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, frequentQuery, topN)
	if err != nil {
		return AccountFrequencyData{}, fmt.Errorf("could not query frequent accounts: %w", err)
	}
	defer rows.Close()

	var frequentAccounts []FrequentAccount
	for rows.Next() {
		var account FrequentAccount
		var addressHex string
		var isContract int
		err := rows.Scan(&addressHex, &account.AccessCount, &isContract)
		if err != nil {
			return AccountFrequencyData{}, fmt.Errorf("could not scan frequent account: %w", err)
		}
		account.Address = "0x" + addressHex
		account.IsContract = isContract == 1
		frequentAccounts = append(frequentAccounts, account)
	}

	return AccountFrequencyData{
		AverageFrequency:     avgFreq,
		MedianFrequency:      medianFreq,
		MostFrequentAccounts: frequentAccounts,
	}, nil
}

// getMostFrequentStorage gets most frequently accessed storage slots
func (r *ClickHouseRepository) getMostFrequentStorage(ctx context.Context, topN int) (StorageFrequencyData, error) {
	query := `
	WITH access_counts AS (
		SELECT
			address,
			slot_key,
			countMerge(access_count) AS access_count
		FROM mv_storage_access_count
		GROUP BY address, slot_key
	)
	SELECT 
		avg(access_count) as avg_frequency,
		quantile(0.5)(access_count) as median_frequency
	FROM access_counts
	`

	var avgFreq, medianFreq float64
	err := r.db.QueryRowContext(ctx, query).Scan(&avgFreq, &medianFreq)
	if err != nil {
		return StorageFrequencyData{}, fmt.Errorf("could not get storage frequency stats: %w", err)
	}

	// Get most frequent storage slots
	frequentQuery := `
	WITH access_counts AS (
		SELECT
			address,
			slot_key,
			countMerge(access_count) AS access_count
		FROM mv_storage_access_count
		GROUP BY address, slot_key
	)
	SELECT 
		lower(hex(address)) as address,
		lower(hex(slot_key)) as storage_slot,
		access_count
	FROM access_counts
	ORDER BY access_count DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, frequentQuery, topN)
	if err != nil {
		return StorageFrequencyData{}, fmt.Errorf("could not query frequent storage: %w", err)
	}
	defer rows.Close()

	var frequentStorage []FrequentStorage
	for rows.Next() {
		var storage FrequentStorage
		var addressHex, slotHex string
		err := rows.Scan(&addressHex, &slotHex, &storage.AccessCount)
		if err != nil {
			return StorageFrequencyData{}, fmt.Errorf("could not scan frequent storage: %w", err)
		}
		storage.Address = "0x" + addressHex
		storage.StorageSlot = "0x" + slotHex
		frequentStorage = append(frequentStorage, storage)
	}

	return StorageFrequencyData{
		AverageFrequency:  avgFreq,
		MedianFrequency:   medianFreq,
		MostFrequentSlots: frequentStorage,
	}, nil
}

// GetUnifiedAnalytics - All Questions 1-15
func (r *ClickHouseRepository) GetUnifiedAnalytics(ctx context.Context, params QueryParams) (*UnifiedAnalytics, error) {
	log := logger.GetLogger("clickhouse-repo")
	startTime := time.Now()

	// Get all analytics components in parallel (if needed)
	accountAnalytics, err := r.GetAccountAnalytics(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get account analytics: %w", err)
	}

	storageAnalytics, err := r.GetStorageAnalytics(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get storage analytics: %w", err)
	}

	contractAnalytics, err := r.GetContractAnalytics(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get contract analytics: %w", err)
	}

	blockActivityAnalytics, err := r.GetBlockActivityAnalytics(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("could not get block activity analytics: %w", err)
	}

	result := &UnifiedAnalytics{
		Accounts:      *accountAnalytics,
		Storage:       *storageAnalytics,
		Contracts:     *contractAnalytics,
		BlockActivity: *blockActivityAnalytics,
		Metadata: AnalyticsMetadata{
			ExpiryBlock:   params.ExpiryBlock,
			CurrentBlock:  params.CurrentBlock,
			AnalysisRange: params.EndBlock - params.StartBlock,
			GeneratedAt:   time.Now().Unix(),
			QueryDuration: time.Since(startTime).Milliseconds(),
		},
	}

	log.Debug("Retrieved unified analytics",
		"expiry_block", params.ExpiryBlock,
		"duration_ms", time.Since(startTime).Milliseconds())

	return result, nil
}

// ==============================================================================
// SPECIALIZED EFFICIENT QUERIES
// ==============================================================================

// GetBasicStats gets basic statistics for quick overview
func (r *ClickHouseRepository) GetBasicStats(ctx context.Context, expiryBlock uint64) (*BasicStats, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Single query to get basic stats
	query := `
	SELECT 
		countIf(is_contract = 0) as total_eoas,
		countIf(is_contract = 1) as total_contracts,
		countIf(is_contract = 0 AND last_access_block <= ?) as expired_eoas,
		countIf(is_contract = 1 AND last_access_block <= ?) as expired_contracts,
		(SELECT COUNT(*) FROM storage_state) as total_slots,
		(SELECT countIf(last_access_block <= ?) FROM storage_state) as expired_slots
	FROM accounts_state
	`

	var totalEOAs, totalContracts, expiredEOAs, expiredContracts, totalSlots, expiredSlots int

	err := r.db.QueryRowContext(ctx, query, expiryBlock, expiryBlock, expiryBlock).Scan(
		&totalEOAs, &totalContracts, &expiredEOAs, &expiredContracts, &totalSlots, &expiredSlots,
	)
	if err != nil {
		log.Error("Could not get basic stats", "error", err)
		return nil, fmt.Errorf("could not get basic stats: %w", err)
	}

	result := &BasicStats{
		Accounts: BasicAccountStats{
			TotalEOAs:        totalEOAs,
			TotalContracts:   totalContracts,
			ExpiredEOAs:      expiredEOAs,
			ExpiredContracts: expiredContracts,
		},
		Storage: BasicStorageStats{
			TotalSlots:   totalSlots,
			ExpiredSlots: expiredSlots,
		},
		Metadata: BasicMetadata{
			ExpiryBlock: expiryBlock,
			GeneratedAt: time.Now().Unix(),
		},
	}

	log.Debug("Retrieved basic stats",
		"total_accounts", totalEOAs+totalContracts,
		"total_slots", totalSlots,
		"expiry_block", expiryBlock)

	return result, nil
}

// GetTopContractsByExpiredSlots gets top contracts by expired slots
func (r *ClickHouseRepository) GetTopContractsByExpiredSlots(ctx context.Context, expiryBlock uint64, topN int) ([]ContractRankingItem, error) {
	log := logger.GetLogger("clickhouse-repo")

	query := `
	SELECT 
		lower(hex(s.address)) as address,
		COUNT(*) as total_slots,
		countIf(s.last_access_block <= ?) as expired_slots,
		countIf(s.last_access_block > ?) as active_slots,
		(countIf(s.last_access_block <= ?) / COUNT(*) * 100) as expiry_percentage,
		max(s.last_access_block) as last_access,
		any(a.last_access_block > ?) as is_account_active
	FROM storage_state s
	LEFT JOIN accounts_state a ON s.address = a.address
	GROUP BY s.address
	HAVING expired_slots > 0
	ORDER BY expired_slots DESC, expiry_percentage DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, expiryBlock, expiryBlock, expiryBlock, expiryBlock, topN)
	if err != nil {
		log.Error("Could not query top contracts by expired slots", "error", err)
		return nil, fmt.Errorf("could not query top contracts by expired slots: %w", err)
	}
	defer rows.Close()

	var contracts []ContractRankingItem
	for rows.Next() {
		var contract ContractRankingItem
		var addressHex string
		err := rows.Scan(&addressHex, &contract.TotalSlots, &contract.ExpiredSlots, &contract.ActiveSlots,
			&contract.ExpiryPercentage, &contract.LastAccess, &contract.IsAccountActive)
		if err != nil {
			log.Error("Could not scan contract ranking row", "error", err)
			return nil, fmt.Errorf("could not scan contract ranking row: %w", err)
		}
		contract.Address = "0x" + addressHex
		contracts = append(contracts, contract)
	}

	log.Debug("Retrieved top contracts by expired slots", "count", len(contracts))
	return contracts, nil
}

// GetTopContractsByTotalSlots gets top contracts by total slots
func (r *ClickHouseRepository) GetTopContractsByTotalSlots(ctx context.Context, topN int) ([]ContractRankingItem, error) {
	log := logger.GetLogger("clickhouse-repo")

	query := `
	SELECT 
		lower(hex(s.address)) as address,
		COUNT(*) as total_slots,
		0 as expired_slots,
		COUNT(*) as active_slots,
		0 as expiry_percentage,
		max(s.last_access_block) as last_access,
		1 as is_account_active
	FROM storage_state s
	GROUP BY s.address
	ORDER BY total_slots DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, topN)
	if err != nil {
		log.Error("Could not query top contracts by total slots", "error", err)
		return nil, fmt.Errorf("could not query top contracts by total slots: %w", err)
	}
	defer rows.Close()

	var contracts []ContractRankingItem
	for rows.Next() {
		var contract ContractRankingItem
		var addressHex string
		err := rows.Scan(&addressHex, &contract.TotalSlots, &contract.ExpiredSlots, &contract.ActiveSlots,
			&contract.ExpiryPercentage, &contract.LastAccess, &contract.IsAccountActive)
		if err != nil {
			log.Error("Could not scan contract ranking row", "error", err)
			return nil, fmt.Errorf("could not scan contract ranking row: %w", err)
		}
		contract.Address = "0x" + addressHex
		contracts = append(contracts, contract)
	}

	log.Debug("Retrieved top contracts by total slots", "count", len(contracts))
	return contracts, nil
}

// GetTopActivityBlocks gets top activity blocks using optimized block summary tables
func (r *ClickHouseRepository) GetTopActivityBlocks(ctx context.Context, startBlock, endBlock uint64, topN int) ([]BlockActivity, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Use the optimized block summary tables for better performance
	query := `
	SELECT 
		COALESCE(a.block_number, s.block_number) as block_number,
		COALESCE(a.eoa_accesses, 0) as eoa_accesses,
		COALESCE(a.contract_accesses, 0) as contract_accesses,
		COALESCE(a.eoa_accesses, 0) + COALESCE(a.contract_accesses, 0) as account_accesses,
		COALESCE(s.storage_accesses, 0) as storage_accesses,
		COALESCE(a.eoa_accesses, 0) + COALESCE(a.contract_accesses, 0) + COALESCE(s.storage_accesses, 0) as total_accesses
	FROM (
		SELECT 
			block_number,
			sum(eoa_accesses) as eoa_accesses,
			sum(contract_accesses) as contract_accesses
		FROM accounts_block_summary
		WHERE block_number >= ? AND block_number <= ?
		GROUP BY block_number
	) a
	FULL OUTER JOIN (
		SELECT 
			block_number,
			sum(storage_accesses) as storage_accesses
		FROM storage_block_summary
		WHERE block_number >= ? AND block_number <= ?
		GROUP BY block_number
	) s ON a.block_number = s.block_number
	ORDER BY total_accesses DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, startBlock, endBlock, startBlock, endBlock, topN)
	if err != nil {
		log.Error("Could not query top activity blocks", "error", err)
		return nil, fmt.Errorf("could not query top activity blocks: %w", err)
	}
	defer rows.Close()

	var blocks []BlockActivity
	for rows.Next() {
		var block BlockActivity
		err := rows.Scan(&block.BlockNumber, &block.EOAAccesses, &block.ContractAccesses,
			&block.AccountAccesses, &block.StorageAccesses, &block.TotalAccesses)
		if err != nil {
			log.Error("Could not scan block activity row", "error", err)
			return nil, fmt.Errorf("could not scan block activity row: %w", err)
		}
		blocks = append(blocks, block)
	}

	log.Debug("Retrieved top activity blocks", "count", len(blocks))
	return blocks, nil
}

// GetMostFrequentAccounts gets most frequently accessed accounts
func (r *ClickHouseRepository) GetMostFrequentAccounts(ctx context.Context, topN int) ([]FrequentAccount, error) {
	log := logger.GetLogger("clickhouse-repo")

	query := `
	WITH access_counts AS (
		SELECT
			address,
			argMaxMerge(is_contract_state) AS is_contract,
			countMerge(access_count) AS access_count
		FROM account_access_count_agg
		GROUP BY address
	)
	SELECT 
		lower(hex(address)) as address,
		access_count,
		is_contract
	FROM access_counts
	ORDER BY access_count DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, topN)
	if err != nil {
		log.Error("Could not query most frequent accounts", "error", err)
		return nil, fmt.Errorf("could not query most frequent accounts: %w", err)
	}
	defer rows.Close()

	var accounts []FrequentAccount
	for rows.Next() {
		var account FrequentAccount
		var addressHex string
		var isContract int
		err := rows.Scan(&addressHex, &account.AccessCount, &isContract)
		if err != nil {
			log.Error("Could not scan frequent account row", "error", err)
			return nil, fmt.Errorf("could not scan frequent account row: %w", err)
		}
		account.Address = "0x" + addressHex
		account.IsContract = isContract == 1
		accounts = append(accounts, account)
	}

	log.Debug("Retrieved most frequent accounts", "count", len(accounts))
	return accounts, nil
}

// GetMostFrequentStorage gets most frequently accessed storage slots
func (r *ClickHouseRepository) GetMostFrequentStorage(ctx context.Context, topN int) ([]FrequentStorage, error) {
	log := logger.GetLogger("clickhouse-repo")

	query := `
	WITH access_counts AS (
		SELECT
			address,
			slot_key,
			countMerge(access_count) AS access_count
		FROM storage_access_count_agg
		GROUP BY address, slot_key
	)
	SELECT 
		lower(hex(address)) as address,
		lower(hex(slot_key)) as storage_slot,
		access_count
	FROM access_counts
	ORDER BY access_count DESC
	LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, topN)
	if err != nil {
		log.Error("Could not query most frequent storage", "error", err)
		return nil, fmt.Errorf("could not query most frequent storage: %w", err)
	}
	defer rows.Close()

	var storage []FrequentStorage
	for rows.Next() {
		var slot FrequentStorage
		var addressHex, slotHex string
		err := rows.Scan(&addressHex, &slotHex, &slot.AccessCount)
		if err != nil {
			log.Error("Could not scan frequent storage row", "error", err)
			return nil, fmt.Errorf("could not scan frequent storage row: %w", err)
		}
		slot.Address = "0x" + addressHex
		slot.StorageSlot = "0x" + slotHex
		storage = append(storage, slot)
	}

	log.Debug("Retrieved most frequent storage", "count", len(storage))
	return storage, nil
}

// GetTimeSeriesData gets time series data for access patterns using optimized block summary tables
func (r *ClickHouseRepository) GetTimeSeriesData(ctx context.Context, startBlock, endBlock uint64, windowSize int) ([]TimeSeriesPoint, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Use the optimized combined_block_summary table for better performance
	// TODO: fix this using accounts_block_summary and storage_block_summary
	query := `
	SELECT 
		intDiv(block_number, ?) * ? as window_start,
		(intDiv(block_number, ?) + 1) * ? as window_end,
		sum(account_access_count) as account_accesses,
		sum(storage_access_count) as storage_accesses,
		sum(account_access_count) + sum(storage_access_count) as total_accesses,
		(sum(account_access_count) + sum(storage_access_count)) / ? as accesses_per_block
	FROM combined_block_summary
	WHERE block_number >= ? AND block_number <= ?
	GROUP BY window_start, window_end
	ORDER BY window_start
	`

	rows, err := r.db.QueryContext(ctx, query, windowSize, windowSize, windowSize, windowSize,
		windowSize, startBlock, endBlock)
	if err != nil {
		log.Error("Could not query time series data", "error", err)
		return nil, fmt.Errorf("could not query time series data: %w", err)
	}
	defer rows.Close()

	var timeSeriesData []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		err := rows.Scan(&point.WindowStart, &point.WindowEnd, &point.AccountAccesses,
			&point.StorageAccesses, &point.TotalAccesses, &point.AccessesPerBlock)
		if err != nil {
			log.Error("Could not scan time series row", "error", err)
			return nil, fmt.Errorf("could not scan time series row: %w", err)
		}
		timeSeriesData = append(timeSeriesData, point)
	}

	log.Debug("Retrieved time series data", "count", len(timeSeriesData))
	return timeSeriesData, nil
}

// GetAccessRates gets access rate analysis using optimized block summary tables
func (r *ClickHouseRepository) GetAccessRates(ctx context.Context, startBlock, endBlock uint64) (*AccessRateAnalysis, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Use the optimized combined_block_summary table for better performance
	// TODO: fix this using accounts_block_summary and storage_block_summary
	query := `
	SELECT 
		avg(account_access_count) as avg_accounts_per_block,
		avg(storage_access_count) as avg_storage_per_block,
		avg(account_access_count + storage_access_count) as avg_total_per_block,
		COUNT(*) as blocks_analyzed
	FROM combined_block_summary
	WHERE block_number >= ? AND block_number <= ?
	`

	var avgAccountsPerBlock, avgStoragePerBlock, avgTotalPerBlock float64
	var blocksAnalyzed int

	err := r.db.QueryRowContext(ctx, query, startBlock, endBlock).Scan(
		&avgAccountsPerBlock, &avgStoragePerBlock, &avgTotalPerBlock, &blocksAnalyzed,
	)
	if err != nil {
		log.Error("Could not get access rates", "error", err)
		return nil, fmt.Errorf("could not get access rates: %w", err)
	}

	result := &AccessRateAnalysis{
		AccountsPerBlock:      avgAccountsPerBlock,
		StoragePerBlock:       avgStoragePerBlock,
		TotalAccessesPerBlock: avgTotalPerBlock,
		BlocksAnalyzed:        blocksAnalyzed,
	}

	log.Debug("Retrieved access rates", "blocks_analyzed", blocksAnalyzed)
	return result, nil
}

// GetTrendAnalysis gets trend analysis using optimized block summary tables
func (r *ClickHouseRepository) GetTrendAnalysis(ctx context.Context, startBlock, endBlock uint64) (*TrendAnalysis, error) {
	log := logger.GetLogger("clickhouse-repo")

	// Use the optimized combined_block_summary table for better performance
	// TODO: fix this using accounts_block_summary and storage_block_summary
	query := `
	WITH block_activity AS (
		SELECT 
			block_number,
			account_access_count + storage_access_count as total_activity
		FROM combined_block_summary
		WHERE block_number >= ? AND block_number <= ?
		ORDER BY block_number
	),
	trend_stats AS (
		SELECT 
			argMin(total_activity, block_number) as first_activity,
			argMax(total_activity, block_number) as last_activity,
			argMax(block_number, total_activity) as peak_activity_block,
			argMin(block_number, total_activity) as low_activity_block
		FROM block_activity
	)
	SELECT 
		CASE 
			WHEN last_activity > first_activity * 1.1 THEN 'increasing'
			WHEN last_activity < first_activity * 0.9 THEN 'decreasing'
			ELSE 'stable'
		END as trend_direction,
		CASE 
			WHEN first_activity > 0 THEN (last_activity - first_activity) / first_activity * 100
			ELSE 0
		END as growth_rate,
		peak_activity_block,
		low_activity_block
	FROM trend_stats
	LIMIT 1
	`

	var trendDirection string
	var growthRate float64
	var peakActivityBlock, lowActivityBlock uint64

	err := r.db.QueryRowContext(ctx, query, startBlock, endBlock).Scan(
		&trendDirection, &growthRate, &peakActivityBlock, &lowActivityBlock,
	)
	if err != nil {
		log.Error("Could not get trend analysis", "error", err)
		return nil, fmt.Errorf("could not get trend analysis: %w", err)
	}

	result := &TrendAnalysis{
		TrendDirection:    trendDirection,
		GrowthRate:        growthRate,
		PeakActivityBlock: peakActivityBlock,
		LowActivityBlock:  lowActivityBlock,
	}

	log.Debug("Retrieved trend analysis", "trend", trendDirection, "growth_rate", growthRate)
	return result, nil
}

// GetContractExpiryDistribution gets contract expiry distribution
func (r *ClickHouseRepository) GetContractExpiryDistribution(ctx context.Context, expiryBlock uint64) ([]ExpiryDistributionBucket, error) {
	return r.getExpiryDistributionBuckets(ctx, QueryParams{ExpiryBlock: expiryBlock})
}

// GetContractStatusBreakdown gets contract status breakdown
func (r *ClickHouseRepository) GetContractStatusBreakdown(ctx context.Context, expiryBlock uint64) (*ContractStatusAnalysis, error) {
	result, err := r.getContractStatusAnalysis(ctx, QueryParams{ExpiryBlock: expiryBlock})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
