package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

const (
	// pgMaxParams is the maximum number of parameters a postgres query can have.
	pgMaxParams = 65535
	// paramsPerUpsert is the number of parameters for each item in the upsert queries (both accounts and storage).
	paramsPerUpsert = 3
	// batchSize is the number of items to upsert in a single batch.
	batchSize = pgMaxParams / paramsPerUpsert
)

type Contract struct {
	Address   string `json:"address"`
	SlotCount int    `json:"slot_count"`
}

type Account struct {
	Address         string `json:"address"`
	LastAccessBlock uint64 `json:"last_access_block"`
	IsContract      *bool  `json:"is_contract,omitempty"`
}

// Analytics data structures for comprehensive state expiry analysis
type AnalyticsData struct {
	AccountExpiry                 AccountExpiryAnalysis                 `json:"account_expiry"`
	AccountDistribution           AccountDistributionAnalysis           `json:"account_distribution"`
	StorageSlotExpiry             StorageSlotExpiryAnalysis             `json:"storage_slot_expiry"`
	ContractStorage               ContractStorageAnalysis               `json:"contract_storage"`
	StorageExpiry                 StorageExpiryAnalysis                 `json:"storage_expiry"`
	FullyExpiredContracts         FullyExpiredContractsAnalysis         `json:"fully_expired_contracts"`
	ActiveContractsExpiredStorage ActiveContractsExpiredStorageAnalysis `json:"active_contracts_expired_storage"`
	CompleteExpiry                CompleteExpiryAnalysis                `json:"complete_expiry"`
}

// Question 1: How many accounts are expired (separated by EOA and contract)?
type AccountExpiryAnalysis struct {
	ExpiredEOAs               int     `json:"expired_eoas"`
	ExpiredContracts          int     `json:"expired_contracts"`
	TotalExpiredAccounts      int     `json:"total_expired_accounts"`
	TotalEOAs                 int     `json:"total_eoas"`
	TotalContracts            int     `json:"total_contracts"`
	TotalAccounts             int     `json:"total_accounts"`
	ExpiredEOAPercentage      float64 `json:"expired_eoa_percentage"`
	ExpiredContractPercentage float64 `json:"expired_contract_percentage"`
	TotalExpiredPercentage    float64 `json:"total_expired_percentage"`
}

// Question 2: What percentage of expired accounts are contracts vs EOAs?
type AccountDistributionAnalysis struct {
	ContractPercentage   float64 `json:"contract_percentage"`
	EOAPercentage        float64 `json:"eoa_percentage"`
	TotalExpiredAccounts int     `json:"total_expired_accounts"`
}

// New Question: What percentage of storage slots are expired?
type StorageSlotExpiryAnalysis struct {
	ExpiredSlots          int     `json:"expired_slots"`
	TotalSlots            int     `json:"total_slots"`
	ExpiredSlotPercentage float64 `json:"expired_slot_percentage"`
}

// Question 4: What are the top 10 contracts with the largest expired state footprint?
type ContractStorageAnalysis struct {
	TopExpiredContracts []ExpiredContract `json:"top_expired_contracts"`
}

type ExpiredContract struct {
	Address          string  `json:"address"`
	ExpiredSlotCount int     `json:"expired_slot_count"`
	TotalSlotCount   int     `json:"total_slot_count"`
	ExpiryPercentage float64 `json:"expiry_percentage"`
}

// Question 5: What percentage of a contract's total storage is expired?
// Question 6: How many contracts where all slots are expired?
type StorageExpiryAnalysis struct {
	AverageExpiryPercentage float64                  `json:"average_expiry_percentage"`
	MedianExpiryPercentage  float64                  `json:"median_expiry_percentage"`
	ExpiryDistribution      []ExpiryPercentageBucket `json:"expiry_distribution"`
	ContractsAnalyzed       int                      `json:"contracts_analyzed"`
}

type FullyExpiredContractsAnalysis struct {
	FullyExpiredContractCount int     `json:"fully_expired_contract_count"`
	TotalContractsWithStorage int     `json:"total_contracts_with_storage"`
	FullyExpiredPercentage    float64 `json:"fully_expired_percentage"`
}

type ExpiryPercentageBucket struct {
	RangeStart int `json:"range_start"`
	RangeEnd   int `json:"range_end"`
	Count      int `json:"count"`
}

// Question 8: How many contracts are still active but have expired storage? (Detailed threshold analysis)
type ActiveContractsExpiredStorageAnalysis struct {
	ThresholdAnalysis    []ExpiredStorageThreshold `json:"threshold_analysis"`
	TotalActiveContracts int                       `json:"total_active_contracts"`
}

type ExpiredStorageThreshold struct {
	ThresholdRange     string  `json:"threshold_range"`
	ContractCount      int     `json:"contract_count"`
	PercentageOfActive float64 `json:"percentage_of_active"`
}

// Question 9: How many contracts are fully expired at both account and storage levels?
type CompleteExpiryAnalysis struct {
	FullyExpiredContractCount int     `json:"fully_expired_contract_count"`
	TotalContractsWithStorage int     `json:"total_contracts_with_storage"`
	FullyExpiredPercentage    float64 `json:"fully_expired_percentage"`
}

// PostgreSQLRepository implements StateRepositoryInterface for PostgreSQL
type PostgreSQLRepository struct {
	db *pgxpool.Pool
}

// Ensure PostgreSQLRepository implements StateRepositoryInterface
var _ StateRepositoryInterface = (*PostgreSQLRepository)(nil)

func NewPostgreSQLRepository(db *pgxpool.Pool) *PostgreSQLRepository {
	return &PostgreSQLRepository{db: db}
}

// StateRepository is the legacy name - now an alias for backward compatibility
type StateRepository = PostgreSQLRepository

func NewStateRepository(db *pgxpool.Pool) *StateRepository {
	return NewPostgreSQLRepository(db)
}

func (r *PostgreSQLRepository) GetLastIndexedRange(ctx context.Context) (uint64, error) {
	var value string
	err := r.db.QueryRow(ctx, "SELECT value FROM metadata WHERE key = 'last_indexed_range'").Scan(&value)
	if err != nil {
		if err == pgx.ErrNoRows {
			// This can happen if the metadata table is empty. Assume we start from 0.
			return 0, nil
		}
		return 0, fmt.Errorf("could not get last indexed range: %w", err)
	}

	var rangeNumber uint64
	if _, err := fmt.Sscanf(value, "%d", &rangeNumber); err != nil {
		return 0, fmt.Errorf("could not parse last indexed range value '%s': %w", value, err)
	}

	return rangeNumber, nil
}

func (r *PostgreSQLRepository) updateLastIndexedRangeInTx(ctx context.Context, tx pgx.Tx, rangeNumber uint64) error {
	sql := `INSERT INTO metadata (key, value) VALUES ('last_indexed_range', $1) 
		ON CONFLICT (key) DO UPDATE SET value = $1`
	if _, err := tx.Exec(ctx, sql, fmt.Sprintf("%d", rangeNumber)); err != nil {
		return fmt.Errorf("could not update last indexed range: %w", err)
	}
	return nil
}

// UpdateRangeDataInTx processes all blocks in a range and updates the last indexed range
func (r *PostgreSQLRepository) UpdateRangeDataInTx(ctx context.Context,
	accounts map[string]uint64,
	accountType map[string]bool,
	storage map[string]map[string]uint64,
	rangeNumber uint64,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := r.upsertAccessedAccountsInTx(ctx, tx, accounts, accountType); err != nil {
		return fmt.Errorf("could not upsert accounts: %w", err)
	}

	if err := r.upsertAccessedStorageInTx(ctx, tx, storage); err != nil {
		return fmt.Errorf("could not upsert storage: %w", err)
	}

	// Update the last indexed range only after all blocks are processed successfully
	if err := r.updateLastIndexedRangeInTx(ctx, tx, rangeNumber); err != nil {
		return fmt.Errorf("could not update last indexed range: %w", err)
	}

	return tx.Commit(ctx)
}

// BlockData represents the processed data for a single block
type BlockData struct {
	BlockNumber uint64
	Accounts    map[string]bool
	Storage     map[string]map[string]struct{}
}

func (r *PostgreSQLRepository) upsertAccessedAccountsInTx(ctx context.Context, tx pgx.Tx, accounts map[string]uint64, accountType map[string]bool) error {
	if len(accounts) == 0 {
		return nil
	}

	type accountToUpsert struct {
		address     string
		blockNumber uint64
	}

	accountsToUpdate := make([]accountToUpsert, 0, len(accounts))
	for acc, blockNumber := range accounts {
		accountsToUpdate = append(accountsToUpdate, accountToUpsert{address: acc, blockNumber: blockNumber})
	}

	sql := `
		INSERT INTO accounts_current (address, last_access_block, is_contract)
		VALUES %s
		ON CONFLICT (address) DO UPDATE
		SET last_access_block = EXCLUDED.last_access_block,
		    is_contract = COALESCE(accounts_current.is_contract, EXCLUDED.is_contract)
		WHERE accounts_current.last_access_block < EXCLUDED.last_access_block;
	`

	for i := 0; i < len(accountsToUpdate); i += batchSize {
		end := i + batchSize
		if end > len(accountsToUpdate) {
			end = len(accountsToUpdate)
		}
		batch := accountsToUpdate[i:end]

		var values []any
		var placeholders []string
		paramIdx := 1
		for _, account := range batch {
			addr, err := utils.HexToBytes(account.address)
			if err != nil {
				continue
			}
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", paramIdx, paramIdx+1, paramIdx+2))
			values = append(values, addr, account.blockNumber, accountType[account.address])
			paramIdx += 3
		}

		if len(placeholders) == 0 {
			continue // Nothing to insert in this batch
		}

		query := fmt.Sprintf(sql, strings.Join(placeholders, ","))

		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("could not upsert accessed accounts in tx: %w", err)
		}
	}

	return nil
}

func (r *PostgreSQLRepository) upsertAccessedStorageInTx(ctx context.Context, tx pgx.Tx, storage map[string]map[string]uint64) error {
	if len(storage) == 0 {
		return nil
	}

	type storageToUpsert struct {
		address     string
		slot        string
		blockNumber uint64
	}
	// Initial capacity can be a guess. len(storage) is number of addresses, not total slots.
	storageToUpdate := make([]storageToUpsert, 0, len(storage))
	for addr, slots := range storage {
		for slot, blockNumber := range slots {
			storageToUpdate = append(storageToUpdate, storageToUpsert{address: addr, slot: slot, blockNumber: blockNumber})
		}
	}

	sql := `
		INSERT INTO storage_current (address, slot_key, last_access_block)
		VALUES %s
		ON CONFLICT (address, slot_key) DO UPDATE
		SET last_access_block = EXCLUDED.last_access_block
		WHERE storage_current.last_access_block < EXCLUDED.last_access_block;
	`

	for i := 0; i < len(storageToUpdate); i += batchSize {
		end := i + batchSize
		if end > len(storageToUpdate) {
			end = len(storageToUpdate)
		}
		batch := storageToUpdate[i:end]

		var values []any
		var placeholders []string
		paramIdx := 1
		for _, s := range batch {
			addressBytes, err := utils.HexToBytes(s.address)
			if err != nil {
				continue
			}
			slotBytes, err := utils.HexToBytes(s.slot)
			if err != nil {
				continue
			}
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", paramIdx, paramIdx+1, paramIdx+2))
			values = append(values, addressBytes, slotBytes, s.blockNumber)
			paramIdx += 3
		}

		if len(placeholders) == 0 {
			continue // Nothing to insert in this batch
		}

		query := fmt.Sprintf(sql, strings.Join(placeholders, ","))

		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("could not upsert accessed storage in tx: %w", err)
		}
	}

	return nil
}

func (r *PostgreSQLRepository) GetExpiredStateCount(ctx context.Context, expiryBlock uint64) (int, error) {
	var accountCount int
	accountQuery := `SELECT COUNT(*) FROM accounts_current WHERE last_access_block < $1;`
	err := r.db.QueryRow(ctx, accountQuery, expiryBlock).Scan(&accountCount)
	if err != nil {
		return 0, fmt.Errorf("could not get expired account count: %w", err)
	}

	var storageCount int
	storageQuery := `SELECT COUNT(*) FROM storage_current WHERE last_access_block < $1;`
	err = r.db.QueryRow(ctx, storageQuery, expiryBlock).Scan(&storageCount)
	if err != nil {
		return 0, fmt.Errorf("could not get expired storage count: %w", err)
	}

	return accountCount + storageCount, nil
}

func (r *PostgreSQLRepository) GetTopNExpiredContracts(ctx context.Context, expiryBlock uint64, n int) ([]Contract, error) {
	query := `
		SELECT
			address,
			COUNT(slot_key) as slot_count
		FROM
			storage_current
		WHERE
			last_access_block < $1
		GROUP BY
			address
		ORDER BY
			slot_count DESC
		LIMIT $2;
	`

	rows, err := r.db.Query(ctx, query, expiryBlock, n)
	if err != nil {
		return nil, fmt.Errorf("could not query for top expired contracts: %w", err)
	}
	defer rows.Close()

	var contracts []Contract
	for rows.Next() {
		var contract Contract
		var addressBytes []byte
		if err := rows.Scan(&addressBytes, &contract.SlotCount); err != nil {
			return nil, fmt.Errorf("could not scan contract row: %w", err)
		}
		contract.Address = utils.BytesToHex(addressBytes)
		contracts = append(contracts, contract)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over contract rows: %w", err)
	}

	return contracts, nil
}

func (r *PostgreSQLRepository) GetStateLastAccessedBlock(ctx context.Context, address string, slot *string) (uint64, error) {
	var lastAccessBlock uint64
	var err error

	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		return 0, fmt.Errorf("invalid address hex: %w", err)
	}

	if slot == nil {
		// Query for account
		query := `SELECT last_access_block FROM accounts_current WHERE address = $1;`
		err = r.db.QueryRow(ctx, query, addressBytes).Scan(&lastAccessBlock)
	} else {
		// Query for storage slot
		slotBytes, err := utils.HexToBytes(*slot)
		if err != nil {
			return 0, fmt.Errorf("invalid slot hex: %w", err)
		}
		query := `SELECT last_access_block FROM storage_current WHERE address = $1 AND slot_key = $2;`
		err = r.db.QueryRow(ctx, query, addressBytes, slotBytes).Scan(&lastAccessBlock)
	}

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil // Not found
		}
		return 0, fmt.Errorf("could not get last access block: %w", err)
	}

	return lastAccessBlock, nil
}

func (r *PostgreSQLRepository) GetAccountType(ctx context.Context, address string) (*bool, error) {
	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address hex: %w", err)
	}

	var isContract *bool
	query := `SELECT is_contract FROM accounts_current WHERE address = $1;`
	err = r.db.QueryRow(ctx, query, addressBytes).Scan(&isContract)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // Account not found
		}
		return nil, fmt.Errorf("could not get account type: %w", err)
	}

	return isContract, nil
}

func (r *PostgreSQLRepository) GetAccountInfo(ctx context.Context, address string) (*Account, error) {
	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address hex: %w", err)
	}

	var account Account
	var isContract *bool
	var lastAccessBlock uint64
	query := `SELECT last_access_block, is_contract FROM accounts_current WHERE address = $1;`
	err = r.db.QueryRow(ctx, query, addressBytes).Scan(&lastAccessBlock, &isContract)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // Account not found
		}
		return nil, fmt.Errorf("could not get account info: %w", err)
	}

	account.Address = address
	account.LastAccessBlock = lastAccessBlock
	account.IsContract = isContract

	return &account, nil
}

func (r *PostgreSQLRepository) GetExpiredAccountsByType(ctx context.Context, expiryBlock uint64, isContract *bool) ([]Account, error) {
	var query string
	var args []any

	if isContract == nil {
		query = `SELECT address, last_access_block, is_contract FROM accounts_current WHERE last_access_block < $1;`
		args = []any{expiryBlock}
	} else {
		query = `SELECT address, last_access_block, is_contract FROM accounts_current WHERE last_access_block < $1 AND is_contract = $2;`
		args = []any{expiryBlock, *isContract}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("could not query expired accounts by type: %w", err)
	}
	defer rows.Close()

	var accounts []Account
	for rows.Next() {
		var account Account
		var addressBytes []byte
		var isContractVal *bool
		if err := rows.Scan(&addressBytes, &account.LastAccessBlock, &isContractVal); err != nil {
			return nil, fmt.Errorf("could not scan account row: %w", err)
		}
		account.Address = utils.BytesToHex(addressBytes)
		account.IsContract = isContractVal
		accounts = append(accounts, account)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over account rows: %w", err)
	}

	return accounts, nil
}

type SyncStatus struct {
	IsSynced         bool   `json:"is_synced"`
	LastIndexedRange uint64 `json:"last_indexed_range"`
	EndBlock         uint64 `json:"end_block"`
}

func (r *PostgreSQLRepository) GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error) {
	lastIndexedRange, err := r.GetLastIndexedRange(ctx)
	if err != nil {
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

	return &SyncStatus{
		IsSynced:         isSynced,
		LastIndexedRange: lastIndexedRange,
		EndBlock:         endBlock,
	}, nil
}

// GetAnalyticsData returns comprehensive analytics for all questions with optimized single-query approach
// This method executes a single comprehensive base query and derives all analytics for maximum efficiency
func (r *PostgreSQLRepository) GetAnalyticsData(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*AnalyticsData, error) {
	analytics := &AnalyticsData{}

	// Get base statistics with a single optimized query
	baseStats, err := r.getBaseStatistics(ctx, expiryBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to get base statistics: %w", err)
	}

	// Derive all analytics from base statistics (much more efficient)
	analytics.AccountExpiry = r.deriveAccountExpiryAnalysis(baseStats)
	analytics.AccountDistribution = r.deriveAccountDistributionAnalysis(baseStats)
	analytics.StorageSlotExpiry = r.deriveStorageSlotExpiryAnalysis(baseStats)

	// Get contract storage analysis (still needs separate query for top 10)
	if err := r.getContractStorageAnalysis(ctx, expiryBlock, &analytics.ContractStorage); err != nil {
		return nil, fmt.Errorf("failed to get contract storage analysis: %w", err)
	}

	// Get storage expiry analysis and fully expired contracts (combined for efficiency)
	if err := r.getStorageExpiryAnalysis(ctx, expiryBlock, &analytics.StorageExpiry, &analytics.FullyExpiredContracts); err != nil {
		return nil, fmt.Errorf("failed to get storage expiry analysis: %w", err)
	}

	// Temporarily skip active contracts with expired storage analysis
	// Set default empty values to avoid nil in response
	analytics.ActiveContractsExpiredStorage = ActiveContractsExpiredStorageAnalysis{
		ThresholdAnalysis:    []ExpiredStorageThreshold{},
		TotalActiveContracts: 0,
	}

	// Get complete expiry analysis
	if err := r.getCompleteExpiryAnalysis(ctx, expiryBlock, &analytics.CompleteExpiry); err != nil {
		return nil, fmt.Errorf("failed to get complete expiry analysis: %w", err)
	}

	return analytics, nil
}

// BaseStatistics holds all basic counts that can be derived from a single query
type BaseStatistics struct {
	// Account statistics (derived totals calculated via methods)
	TotalEOAs        int
	TotalContracts   int
	ExpiredEOAs      int
	ExpiredContracts int

	// Storage statistics
	TotalSlots   int
	ExpiredSlots int
}

// TotalAccounts returns the total count of all accounts (derived)
func (bs *BaseStatistics) TotalAccounts() int {
	return bs.TotalEOAs + bs.TotalContracts
}

// ExpiredAccounts returns the total count of expired accounts (derived)
func (bs *BaseStatistics) ExpiredAccounts() int {
	return bs.ExpiredEOAs + bs.ExpiredContracts
}

// getBaseStatistics retrieves all basic statistics in a single optimized query
func (r *PostgreSQLRepository) getBaseStatistics(ctx context.Context, expiryBlock uint64) (*BaseStatistics, error) {
	query := `
		WITH account_stats AS (
			SELECT 
				COUNT(*) FILTER (WHERE is_contract = false) as total_eoas,
				COUNT(*) FILTER (WHERE is_contract = true) as total_contracts,
				COUNT(*) FILTER (WHERE last_access_block < $1 AND is_contract = false) as expired_eoas,
				COUNT(*) FILTER (WHERE last_access_block < $1 AND is_contract = true) as expired_contracts
			FROM accounts_current
		),
		storage_stats AS (
			SELECT 
				COUNT(*) as total_slots,
				COUNT(*) FILTER (WHERE last_access_block < $1) as expired_slots
			FROM storage_current
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
	err := r.db.QueryRow(ctx, query, expiryBlock).Scan(
		&stats.TotalEOAs,
		&stats.TotalContracts,
		&stats.ExpiredEOAs,
		&stats.ExpiredContracts,
		&stats.TotalSlots,
		&stats.ExpiredSlots,
	)
	if err != nil {
		return nil, fmt.Errorf("could not get base statistics: %w", err)
	}

	return &stats, nil
}

// deriveAccountExpiryAnalysis derives account expiry analysis from base statistics
func (r *PostgreSQLRepository) deriveAccountExpiryAnalysis(stats *BaseStatistics) AccountExpiryAnalysis {
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
func (r *PostgreSQLRepository) deriveAccountDistributionAnalysis(stats *BaseStatistics) AccountDistributionAnalysis {
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
func (r *PostgreSQLRepository) deriveStorageSlotExpiryAnalysis(stats *BaseStatistics) StorageSlotExpiryAnalysis {
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

// Question 4: What are the top 10 contracts with the largest expired state footprint?
func (r *PostgreSQLRepository) getContractStorageAnalysis(ctx context.Context, expiryBlock uint64, result *ContractStorageAnalysis) error {
	query := `
		WITH contract_storage_stats AS (
			SELECT 
				s.address,
				COUNT(*) FILTER (WHERE s.last_access_block < $1) as expired_slots,
				COUNT(*) as total_slots
			FROM storage_current s
			GROUP BY s.address
			HAVING COUNT(*) FILTER (WHERE s.last_access_block < $1) > 0
		)
		SELECT 
			address,
			expired_slots,
			total_slots,
			(expired_slots::float / total_slots::float * 100) as expiry_percentage
		FROM contract_storage_stats
		ORDER BY expired_slots DESC, expiry_percentage DESC
		LIMIT 10
	`

	rows, err := r.db.Query(ctx, query, expiryBlock)
	if err != nil {
		return fmt.Errorf("could not query contract storage analysis: %w", err)
	}
	defer rows.Close()

	var contracts []ExpiredContract
	for rows.Next() {
		var contract ExpiredContract
		var addressBytes []byte

		err := rows.Scan(
			&addressBytes,
			&contract.ExpiredSlotCount,
			&contract.TotalSlotCount,
			&contract.ExpiryPercentage,
		)
		if err != nil {
			return fmt.Errorf("could not scan contract storage row: %w", err)
		}

		contract.Address = utils.BytesToHex(addressBytes)
		contracts = append(contracts, contract)
	}

	result.TopExpiredContracts = contracts
	return rows.Err()
}

// Questions 5 & 6: Storage expiry analysis and fully expired contracts (Optimized)
func (r *PostgreSQLRepository) getStorageExpiryAnalysis(ctx context.Context, expiryBlock uint64, storageResult *StorageExpiryAnalysis, fullyExpiredResult *FullyExpiredContractsAnalysis) error {
	// Simplified query that avoids complex JSON aggregation
	query := `
		WITH contract_expiry_stats AS (
			SELECT 
				(COUNT(*) FILTER (WHERE s.last_access_block < $1)::float / COUNT(*)::float * 100) as expiry_percentage
			FROM storage_current s
			GROUP BY s.address
		)
		SELECT 
			AVG(expiry_percentage) as avg_expiry_percentage,
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expiry_percentage) as median_expiry_percentage,
			COUNT(*) as contracts_analyzed,
			COUNT(*) FILTER (WHERE expiry_percentage = 100) as fully_expired_count
		FROM contract_expiry_stats
	`

	var avgExpiry, medianExpiry float64
	var contractsAnalyzed, fullyExpiredCount int

	err := r.db.QueryRow(ctx, query, expiryBlock).Scan(
		&avgExpiry,
		&medianExpiry,
		&contractsAnalyzed,
		&fullyExpiredCount,
	)
	if err != nil {
		return fmt.Errorf("could not get storage expiry analysis: %w", err)
	}

	storageResult.AverageExpiryPercentage = avgExpiry
	storageResult.MedianExpiryPercentage = medianExpiry
	storageResult.ContractsAnalyzed = contractsAnalyzed

	// Get distribution buckets with a separate, simpler query
	buckets, err := r.getExpiryDistributionBuckets(ctx, expiryBlock)
	if err != nil {
		return fmt.Errorf("could not get expiry distribution buckets: %w", err)
	}
	storageResult.ExpiryDistribution = buckets

	fullyExpiredResult.FullyExpiredContractCount = fullyExpiredCount
	fullyExpiredResult.TotalContractsWithStorage = contractsAnalyzed
	if contractsAnalyzed > 0 {
		fullyExpiredResult.FullyExpiredPercentage = float64(fullyExpiredCount) / float64(contractsAnalyzed) * 100
	}

	return nil
}

// getExpiryDistributionBuckets gets distribution buckets with a simpler query
func (r *PostgreSQLRepository) getExpiryDistributionBuckets(ctx context.Context, expiryBlock uint64) ([]ExpiryPercentageBucket, error) {
	query := `
		WITH contract_expiry_stats AS (
			SELECT 
				(COUNT(*) FILTER (WHERE s.last_access_block < $1)::float / COUNT(*)::float * 100) as expiry_percentage
			FROM storage_current s
			GROUP BY s.address
		),
		bucketed_stats AS (
			SELECT 
				CASE 
					WHEN expiry_percentage = 0 THEN 0
					WHEN expiry_percentage <= 10 THEN 10
					WHEN expiry_percentage <= 20 THEN 20
					WHEN expiry_percentage <= 30 THEN 30
					WHEN expiry_percentage <= 40 THEN 40
					WHEN expiry_percentage <= 50 THEN 50
					WHEN expiry_percentage <= 60 THEN 60
					WHEN expiry_percentage <= 70 THEN 70
					WHEN expiry_percentage <= 80 THEN 80
					WHEN expiry_percentage <= 90 THEN 90
					ELSE 100
				END as bucket
			FROM contract_expiry_stats
		)
		SELECT 
			bucket,
			COUNT(*) as count
		FROM bucketed_stats
		GROUP BY bucket
		ORDER BY bucket
	`

	rows, err := r.db.Query(ctx, query, expiryBlock)
	if err != nil {
		return nil, fmt.Errorf("could not query expiry distribution buckets: %w", err)
	}
	defer rows.Close()

	var buckets []ExpiryPercentageBucket
	for rows.Next() {
		var bucket ExpiryPercentageBucket
		var bucketEnd int

		err := rows.Scan(&bucketEnd, &bucket.Count)
		if err != nil {
			return nil, fmt.Errorf("could not scan bucket row: %w", err)
		}

		// Calculate range start based on bucket end
		if bucketEnd == 0 {
			bucket.RangeStart = 0
		} else {
			bucket.RangeStart = bucketEnd - 9
		}
		bucket.RangeEnd = bucketEnd

		buckets = append(buckets, bucket)
	}

	return buckets, rows.Err()
}

// Question 8: How many contracts are still active but have expired storage? (Detailed threshold analysis)
// NOTE: This function is temporarily disabled due to memory issues with large datasets.
// The GetAnalyticsData method now returns empty data for this section.
func (r *PostgreSQLRepository) getActiveContractsExpiredStorageAnalysis(ctx context.Context, expiryBlock uint64, result *ActiveContractsExpiredStorageAnalysis) error {
	// Memory-efficient approach: Avoid JOIN by using subqueries and window functions
	thresholdQuery := `
		WITH contract_storage_stats AS (
			SELECT 
				s.address,
				COUNT(s.slot_key) as total_slots,
				COUNT(s.slot_key) FILTER (WHERE s.last_access_block < $1) as expired_slots,
				(COUNT(s.slot_key) FILTER (WHERE s.last_access_block < $1)::float / COUNT(s.slot_key)::float * 100) as expiry_percentage
			FROM storage_current s
			WHERE EXISTS (
				SELECT 1 FROM accounts_current a 
				WHERE a.address = s.address 
				AND a.is_contract = true 
				AND a.last_access_block >= $1
			)
			GROUP BY s.address
		),
		threshold_buckets AS (
			SELECT 
				CASE 
					WHEN expiry_percentage = 0 THEN '0%'
					WHEN expiry_percentage > 0 AND expiry_percentage <= 20 THEN '1-20%'
					WHEN expiry_percentage > 20 AND expiry_percentage <= 50 THEN '21-50%'
					WHEN expiry_percentage > 50 AND expiry_percentage <= 80 THEN '51-80%'
					WHEN expiry_percentage > 80 AND expiry_percentage < 100 THEN '81-99%'
					ELSE '100%'
				END as threshold_range
			FROM contract_storage_stats
		)
		SELECT 
			threshold_range,
			COUNT(*) as active_contract_count
		FROM threshold_buckets
		GROUP BY threshold_range
		ORDER BY 
			CASE threshold_range
				WHEN '0%' THEN 1
				WHEN '1-20%' THEN 2
				WHEN '21-50%' THEN 3
				WHEN '51-80%' THEN 4
				WHEN '81-99%' THEN 5
				WHEN '100%' THEN 6
			END
	`

	// Get total active contracts count first - optimized to avoid JOIN and GROUP BY
	totalActiveQuery := `
		SELECT COUNT(DISTINCT a.address) as total_active_contracts
		FROM accounts_current a
		WHERE a.is_contract = true 
		  AND a.last_access_block >= $1
		  AND EXISTS (
			  SELECT 1 FROM storage_current s 
			  WHERE s.address = a.address
		  )
	`

	var totalActiveContracts int
	err := r.db.QueryRow(ctx, totalActiveQuery, expiryBlock).Scan(&totalActiveContracts)
	if err != nil {
		return fmt.Errorf("could not get total active contracts: %w", err)
	}

	// Get threshold analysis rows (memory efficient)
	rows, err := r.db.Query(ctx, thresholdQuery, expiryBlock)
	if err != nil {
		return fmt.Errorf("could not query threshold analysis: %w", err)
	}
	defer rows.Close()

	var thresholdAnalysis []ExpiredStorageThreshold
	for rows.Next() {
		var threshold ExpiredStorageThreshold
		var contractCount int

		err := rows.Scan(&threshold.ThresholdRange, &contractCount)
		if err != nil {
			return fmt.Errorf("could not scan threshold row: %w", err)
		}

		threshold.ContractCount = contractCount
		// Calculate percentage of active contracts
		if totalActiveContracts > 0 {
			threshold.PercentageOfActive = float64(contractCount) / float64(totalActiveContracts) * 100
		} else {
			threshold.PercentageOfActive = 0
		}

		thresholdAnalysis = append(thresholdAnalysis, threshold)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating threshold rows: %w", err)
	}

	result.ThresholdAnalysis = thresholdAnalysis
	result.TotalActiveContracts = totalActiveContracts
	return nil
}

// Question 9: How many contracts are fully expired at both account and storage levels?
func (r *PostgreSQLRepository) getCompleteExpiryAnalysis(ctx context.Context, expiryBlock uint64, result *CompleteExpiryAnalysis) error {
	query := `
		WITH fully_expired_storage_contracts AS (
			SELECT DISTINCT s.address
			FROM storage_current s
			GROUP BY s.address
			HAVING COUNT(*) > 0 AND COUNT(*) FILTER (WHERE s.last_access_block >= $1) = 0
		),
		fully_expired_contracts AS (
			SELECT fesc.address
			FROM fully_expired_storage_contracts fesc
			INNER JOIN accounts_current a ON fesc.address = a.address
			WHERE a.last_access_block < $1 AND a.is_contract = true
		)
		SELECT 
			COUNT(fec.address) as fully_expired_contract_count,
			(SELECT COUNT(DISTINCT s.address) FROM storage_current s) as total_contracts_with_storage
		FROM fully_expired_contracts fec
		RIGHT JOIN (SELECT 1) dummy ON true
	`

	var fullyExpiredCount, totalContracts int

	err := r.db.QueryRow(ctx, query, expiryBlock).Scan(
		&fullyExpiredCount,
		&totalContracts,
	)
	if err != nil {
		return fmt.Errorf("could not get complete expiry analysis: %w", err)
	}

	result.FullyExpiredContractCount = fullyExpiredCount
	result.TotalContractsWithStorage = totalContracts
	if totalContracts > 0 {
		result.FullyExpiredPercentage = float64(fullyExpiredCount) / float64(totalContracts) * 100
	}

	return nil
}
