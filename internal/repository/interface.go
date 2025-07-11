package repository

import (
	"context"
	"fmt"

	"github.com/weiihann/state-expiry-indexer/db"
	"github.com/weiihann/state-expiry-indexer/internal"
)

// StateRepositoryInterface defines efficient methods for ClickHouse queries
// Each method is optimized for specific question categories with minimal database operations
type StateRepositoryInterface interface {
	// Core indexing operations
	GetLastIndexedRange(ctx context.Context) (uint64, error)
	InsertRange(
		ctx context.Context,
		accountAccesses map[uint64]map[string]struct{},
		accountType map[string]bool,
		storageAccesses map[uint64]map[string]map[string]struct{},
		rangeNumber uint64,
	) error
	GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error)

	// ==============================================================================
	// OPTIMIZED ANALYTICS METHODS (Questions 1-15)
	// ==============================================================================

	// Account Analytics (Questions 1, 2, 5a)
	// Single query using accounts_state and account_access_count_agg tables
	GetAccountAnalytics(ctx context.Context, params QueryParams) (*AccountAnalytics, error)

	// Storage Analytics (Questions 3, 4, 5b)
	// Single query using storage_state and storage_access_count_agg tables
	GetStorageAnalytics(ctx context.Context, params QueryParams) (*StorageAnalytics, error)

	// Contract Analytics (Questions 7, 8, 9, 10, 11, 15)
	// Uses contract_storage_count_agg and joins with state tables
	GetContractAnalytics(ctx context.Context, params QueryParams) (*ContractAnalytics, error)

	// Block Activity Analytics (Questions 6, 12, 13, 14)
	// Uses block summary tables for efficient time-series queries
	GetBlockActivityAnalytics(ctx context.Context, params QueryParams) (*BlockActivityAnalytics, error)

	// Unified Analytics (All Questions)
	// Combines all analytics in a single response with parallel queries
	GetUnifiedAnalytics(ctx context.Context, params QueryParams) (*UnifiedAnalytics, error)

	// ==============================================================================
	// SPECIALIZED EFFICIENT QUERIES
	// ==============================================================================

	// Get basic statistics in a single query for quick overview
	GetBasicStats(ctx context.Context, expiryBlock uint64) (*BasicStats, error)

	// Get top N items efficiently with single queries
	GetTopContractsByExpiredSlots(ctx context.Context, expiryBlock uint64, topN int) ([]ContractRankingItem, error)
	GetTopContractsByTotalSlots(ctx context.Context, topN int) ([]ContractRankingItem, error)
	GetTopActivityBlocks(ctx context.Context, startBlock, endBlock uint64, topN int) ([]BlockActivity, error)
	GetMostFrequentAccounts(ctx context.Context, topN int) ([]FrequentAccount, error)
	GetMostFrequentStorage(ctx context.Context, topN int) ([]FrequentStorage, error)

	// Time series queries using pre-aggregated data
	GetAccessRates(ctx context.Context, startBlock, endBlock uint64) (*AccessRateAnalysis, error)
	GetTimeSeriesData(ctx context.Context, startBlock, endBlock uint64, windowSize int) ([]TimeSeriesPoint, error)
	GetTrendAnalysis(ctx context.Context, startBlock, endBlock uint64) (*TrendAnalysis, error)
}

// AdvancedAnalyticsError represents errors for unsupported advanced analytics operations
type AdvancedAnalyticsError struct {
	Operation string
	Message   string
	Database  string
}

func (e *AdvancedAnalyticsError) Error() string {
	return fmt.Sprintf("advanced analytics operation '%s' not supported in %s: %s", e.Operation, e.Database, e.Message)
}

// NewAdvancedAnalyticsError creates a new error for unsupported operations
func NewAdvancedAnalyticsError(operation, database, message string) *AdvancedAnalyticsError {
	return &AdvancedAnalyticsError{
		Operation: operation,
		Database:  database,
		Message:   message,
	}
}

// NewRepository creates the appropriate repository implementation based on configuration
func NewRepository(ctx context.Context, config internal.Config) (StateRepositoryInterface, error) {
	db, err := db.ConnectClickHouseSQL(config)
	if err != nil {
		return nil, err
	}
	return NewClickHouseRepository(db), nil
}
