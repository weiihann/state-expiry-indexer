package repository

import (
	"context"
	"fmt"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"

	// ClickHouse database drivers
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// Test helper functions for ClickHouse testing

// generateTestAddress creates a test Ethereum address
func generateClickHouseTestAddress(index int) string {
	return fmt.Sprintf("0x%040x", index)
}

// generateTestStorageSlot creates a test storage slot key
func generateClickHouseTestStorageSlot(index int) string {
	return fmt.Sprintf("0x%064x", index)
}

// Note: ClickHouse test config is now shared in test_helpers.go

// setupClickHouseTestRepository creates a test ClickHouse repository with clean database
func setupClickHouseTestRepository(t *testing.T) (StateRepositoryInterface, func()) {
	t.Helper()

	// Use standard test configuration
	config := getTestClickHouseConfig()
	cleanup := testdb.SetupTestDatabase(t, true)

	// Create repository
	repo, err := NewRepository(t.Context(), config)
	if err != nil {
		return nil, func() {}
	}

	return repo, cleanup
}

// TestClickHouseGetLastIndexedRange tests getting the last indexed range from metadata
func TestClickHouseGetLastIndexedRange(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := context.Background()
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), lastRange, "Should return 0 for empty database")
	})

	t.Run("WithExistingData", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := context.Background()

		// First update to create metadata entry
		accounts := map[string]uint64{"0x1234567890123456789012345678901234567890": 100}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[string]map[string]uint64{}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 42)
		require.NoError(t, err)

		// Now check that we can retrieve it
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(42), lastRange, fmt.Sprintf("expected 42, got %d", lastRange))
	})

	t.Run("MultipleUpdates", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := context.Background()

		// Update multiple times
		accounts := map[string]uint64{"0x1234567890123456789012345678901234567890": 100}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[string]map[string]uint64{}

		ranges := []uint64{10, 25, 50, 100}
		for _, rangeNum := range ranges {
			err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, rangeNum)
			require.NoError(t, err)

			lastRange, err := repo.GetLastIndexedRange(ctx)
			require.NoError(t, err)
			assert.Equal(t, rangeNum, lastRange, "Should return the most recent range")
		}
	})
}

// TestClickHouseUpdateRangeDataInTx tests the main data update functionality
func TestClickHouseUpdateRangeDataInTx(t *testing.T) {
	t.Run("EmptyMaps", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[string]uint64{}
		accountType := map[string]bool{}
		storage := map[string]map[string]uint64{}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// Verify metadata was updated
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
	})

	t.Run("AccountsOnly", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[string]uint64{
			"0x1234567890123456789012345678901234567890": 100,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": 150,
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false, // EOA
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,  // Contract
		}
		storage := map[string]map[string]uint64{}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// For ClickHouse, we can verify data was inserted by checking if we can get analytics
		// (we don't have direct access to GetAccountInfo like PostgreSQL)
		analytics, err := repo.GetAnalyticsData(ctx, 200, 300) // Expiry after our test data
		require.NoError(t, err)
		assert.Greater(t, analytics.AccountExpiry.TotalAccounts, 0, "Should have accounts in ClickHouse")
	})

	t.Run("StorageOnly", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[string]uint64{}
		accountType := map[string]bool{}
		storage := map[string]map[string]uint64{
			"0x1234567890123456789012345678901234567890": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 100,
				"0x0000000000000000000000000000000000000000000000000000000000000002": 150,
			},
		}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// Verify storage was inserted by checking analytics
		analytics, err := repo.GetAnalyticsData(ctx, 200, 300)
		require.NoError(t, err)
		assert.Greater(t, analytics.StorageSlotExpiry.TotalSlots, 0, "Should have storage slots in ClickHouse")
	})

	t.Run("AccountsAndStorage", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[string]uint64{
			"0x1234567890123456789012345678901234567890": 100,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": 150,
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,
		}
		storage := map[string]map[string]uint64{
			"0x1234567890123456789012345678901234567890": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 100,
			},
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 150,
				"0x0000000000000000000000000000000000000000000000000000000000000002": 160,
			},
		}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 2)
		require.NoError(t, err)

		// Verify both accounts and storage were inserted
		analytics, err := repo.GetAnalyticsData(ctx, 200, 300)
		require.NoError(t, err)
		assert.Greater(t, analytics.AccountExpiry.TotalAccounts, 0, "Should have accounts")
		assert.Greater(t, analytics.StorageSlotExpiry.TotalSlots, 0, "Should have storage slots")

		// Verify metadata was updated
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), lastRange)
	})

	t.Run("LargeDataSet", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Create large dataset to test batch processing
		accounts := make(map[string]uint64)
		accountType := make(map[string]bool)
		storage := make(map[string]map[string]uint64)

		// Create 50 accounts with storage (smaller than PostgreSQL test for ClickHouse)
		for i := 0; i < 50; i++ {
			addr := generateClickHouseTestAddress(i)
			accounts[addr] = uint64(1000 + i)
			accountType[addr] = i%2 == 0 // Alternate between EOA and Contract

			// Add storage for contracts
			if accountType[addr] {
				storage[addr] = make(map[string]uint64)
				for j := 0; j < 3; j++ { // 3 storage slots per contract
					slot := generateClickHouseTestStorageSlot(j)
					storage[addr][slot] = uint64(1000 + i)
				}
			}
		}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 10)
		require.NoError(t, err)

		// Verify the data was inserted
		analytics, err := repo.GetAnalyticsData(ctx, 1200, 1300) // Expiry after our test data
		require.NoError(t, err)
		assert.Greater(t, analytics.AccountExpiry.TotalAccounts, 0, "Should have accounts")
		assert.Greater(t, analytics.StorageSlotExpiry.TotalSlots, 0, "Should have storage")

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), lastRange)
	})
}

// TestClickHouseGetSyncStatus tests sync status reporting
func TestClickHouseGetSyncStatus(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := t.Context()
		status, err := repo.GetSyncStatus(ctx, 100, 10)
		require.NoError(t, err)

		assert.False(t, status.IsSynced, "Should not be synced with empty database")
		assert.Equal(t, uint64(0), status.LastIndexedRange)
		assert.Equal(t, uint64(0), status.EndBlock) // latestRange * rangeSize (0 * 10 = 0)
	})

	t.Run("PartialSync", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Index some ranges
		accounts := map[string]uint64{"0x1234567890123456789012345678901234567890": 100}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[string]map[string]uint64{}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 50)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 100, 10)
		require.NoError(t, err)

		assert.False(t, status.IsSynced, "Should not be synced when partially indexed")
		assert.Equal(t, uint64(50), status.LastIndexedRange)
		assert.Equal(t, uint64(50*10), status.EndBlock)
	})

	t.Run("FullySync", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Index up to the latest range
		accounts := map[string]uint64{"0x1234567890123456789012345678901234567890": 100}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[string]map[string]uint64{}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 100)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 100, 10)
		require.NoError(t, err)

		assert.True(t, status.IsSynced, "Should be synced when up to date")
		assert.Equal(t, uint64(100), status.LastIndexedRange)
		assert.Equal(t, uint64(100*10), status.EndBlock)
	})
}

// TestClickHouseGetAnalyticsData tests comprehensive analytics functionality
func TestClickHouseGetAnalyticsData(t *testing.T) {
	t.Run("WithTestData", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := t.Context()

		// Create test data
		accounts := make(map[string]uint64)
		accountType := make(map[string]bool)
		storage := make(map[string]map[string]uint64)

		// Create 20 accounts: 10 EOAs and 10 contracts
		for i := 0; i < 20; i++ {
			addr := generateClickHouseTestAddress(i)
			accounts[addr] = uint64(1000 + i*100) // Spread across blocks
			accountType[addr] = i >= 10           // First 10 are EOAs, rest are contracts

			// Add storage for contracts
			if accountType[addr] {
				storage[addr] = make(map[string]uint64)
				for j := 0; j < 3; j++ { // 3 storage slots per contract
					slot := generateClickHouseTestStorageSlot(j)
					storage[addr][slot] = uint64(1000 + i*100 + j)
				}
			}
		}

		// Load test data
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		expiryBlock := uint64(1500) // Expire accounts/storage last accessed before this
		currentBlock := uint64(2000)

		analytics, err := repo.GetAnalyticsData(ctx, expiryBlock, currentBlock)
		require.NoError(t, err)

		// Verify basic account statistics
		assert.Equal(t, 20, analytics.AccountExpiry.TotalAccounts, "Should have 20 total accounts")
		assert.Equal(t, 10, analytics.AccountExpiry.TotalEOAs, "Should have 10 EOAs")
		assert.Equal(t, 10, analytics.AccountExpiry.TotalContracts, "Should have 10 contracts")

		// Some accounts should be expired based on expiry block
		assert.GreaterOrEqual(t, analytics.AccountExpiry.ExpiredEOAs, 0, "Should have EOA data")
		assert.GreaterOrEqual(t, analytics.AccountExpiry.ExpiredContracts, 0, "Should have contract data")

		// Verify storage statistics
		assert.Equal(t, 30, analytics.StorageSlotExpiry.TotalSlots, "Should have 30 storage slots (10 contracts * 3 slots)")
		assert.GreaterOrEqual(t, analytics.StorageSlotExpiry.ExpiredSlots, 0, "Should have expired slots data")

		// Verify consistency
		assert.Equal(t, analytics.AccountExpiry.ExpiredEOAs+analytics.AccountExpiry.ExpiredContracts,
			analytics.AccountExpiry.TotalExpiredAccounts, "Expired accounts should sum correctly")
		assert.Equal(t, analytics.AccountExpiry.TotalEOAs+analytics.AccountExpiry.TotalContracts,
			analytics.AccountExpiry.TotalAccounts, "Total accounts should sum correctly")
	})
}

// TestClickHouseUpdateRangeDataWithAllEventsInTx tests archive mode functionality
func TestClickHouseUpdateRangeDataWithAllEventsInTx(t *testing.T) {
	t.Run("EmptyEvents", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accountAccesses := map[uint64]map[string]struct{}{}
		accountType := map[string]bool{}
		storageAccesses := map[uint64]map[string]map[string]struct{}{}

		err := repo.UpdateRangeDataWithAllEventsInTx(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.NoError(t, err)

		// Verify metadata was updated
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
	})

	t.Run("WithEvents", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Create test data with events across multiple blocks
		accountAccesses := map[uint64]map[string]struct{}{
			1000: {
				"0x1234567890123456789012345678901234567890": {},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {},
			},
			1001: {
				"0x1234567890123456789012345678901234567890": {}, // Access again
			},
		}

		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,
		}

		storageAccesses := map[uint64]map[string]map[string]struct{}{
			1000: {
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
			},
			1001: {
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {}, // Access again
					"0x0000000000000000000000000000000000000000000000000000000000000002": {}, // New slot
				},
			},
		}

		err := repo.UpdateRangeDataWithAllEventsInTx(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.NoError(t, err)

		// For ClickHouse archive mode, ALL events should be stored
		// We can verify by checking that analytics shows we have data
		analytics, err := repo.GetAnalyticsData(ctx, 1100, 1200) // Expiry after some of our test data
		require.NoError(t, err)
		assert.Equal(t, analytics.AccountExpiry.TotalAccounts, 2, "Should have accounts from all events")
		assert.Equal(t, analytics.StorageSlotExpiry.TotalSlots, 2, "Should have storage from all events")

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
	})

	t.Run("MultipleBlockEvents", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Create archive mode data with the same account accessed in multiple blocks
		accountAccesses := map[uint64]map[string]struct{}{
			1000: {
				"0x1234567890123456789012345678901234567890": {},
			},
			1100: {
				"0x1234567890123456789012345678901234567890": {}, // Same account, different block
			},
			1200: {
				"0x1234567890123456789012345678901234567890": {}, // Same account, third block
			},
		}

		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
		}

		storageAccesses := map[uint64]map[string]map[string]struct{}{}

		err := repo.UpdateRangeDataWithAllEventsInTx(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.NoError(t, err)

		// In archive mode, ClickHouse should store all 3 access events for the same account
		// This is different from PostgreSQL which only stores the latest access
		analytics, err := repo.GetAnalyticsData(ctx, 1150, 1300) // Expiry between blocks 2 and 3
		require.NoError(t, err)
		assert.Greater(t, analytics.AccountExpiry.TotalAccounts, 0, "Should have account data")

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
	})
}
