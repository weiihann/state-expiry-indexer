package repository

import (
	"context"
	"fmt"

	"github.com/weiihann/state-expiry-indexer/db"
	"github.com/weiihann/state-expiry-indexer/internal"
)

// StateRepositoryInterface defines the contract for state repository implementations
type StateRepositoryInterface interface {
	// Range-based processing methods (used by indexer)
	GetLastIndexedRange(ctx context.Context) (uint64, error)

	// Archive mode method for storing all events (used by indexer in archive mode)
	InsertRange(
		ctx context.Context,
		accountAccesses map[uint64]map[string]struct{},
		accountType map[string]bool,
		storageAccesses map[uint64]map[string]map[string]struct{},
		rangeNumber uint64,
	) error

	// Basic API query methods (used by API server)
	GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error)

	// Extended analytics methods for comprehensive state analysis
	GetAnalyticsData(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*ExtendedAnalyticsData, error)
	GetSingleAccessAnalytics(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*SingleAccessAnalysis, error)
	GetBlockActivityAnalytics(ctx context.Context, startBlock uint64, endBlock uint64, topN int) (*BlockActivityAnalysis, error)
	GetTimeSeriesAnalytics(ctx context.Context, startBlock uint64, endBlock uint64, windowSize int) (*TimeSeriesAnalysis, error)
	GetStorageVolumeAnalytics(ctx context.Context, expiryBlock uint64, currentBlock uint64, topN int) (*StorageVolumeAnalysis, error)
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
