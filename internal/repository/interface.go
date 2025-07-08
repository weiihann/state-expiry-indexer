package repository

import (
	"context"

	"github.com/weiihann/state-expiry-indexer/db"
	"github.com/weiihann/state-expiry-indexer/internal"
)

// StateRepositoryInterface defines the contract for state repository implementations
type StateRepositoryInterface interface {
	// Range-based processing methods (used by indexer)
	GetLastIndexedRange(ctx context.Context) (uint64, error)
	UpdateRangeDataInTx(
		ctx context.Context,
		accounts map[string]uint64,
		accountType map[string]bool,
		storage map[string]map[string]uint64,
		rangeNumber uint64,
	) error

	// Archive mode method for storing all events (used by indexer in archive mode)
	UpdateRangeDataWithAllEventsInTx(
		ctx context.Context,
		accountAccesses map[uint64]map[string]struct{},
		accountType map[string]bool,
		storageAccesses map[uint64]map[string]map[string]struct{},
		rangeNumber uint64,
	) error

	// API query methods (used by API server)
	GetSyncStatus(ctx context.Context, latestRange uint64, rangeSize uint64) (*SyncStatus, error)
	GetAnalyticsData(ctx context.Context, expiryBlock uint64, currentBlock uint64) (*AnalyticsData, error)
}

// NewRepository creates the appropriate repository implementation based on configuration
func NewRepository(ctx context.Context, config internal.Config) (StateRepositoryInterface, error) {
	if config.ArchiveMode {
		// ClickHouse archive mode - use SQL interface for repository compatibility
		db, err := db.ConnectClickHouseSQL(config)
		if err != nil {
			return nil, err
		}
		return NewClickHouseRepository(db), nil
	} else {
		// PostgreSQL default mode
		db, err := db.Connect(ctx, config)
		if err != nil {
			return nil, err
		}
		return NewPostgreSQLRepository(db), nil
	}
}
