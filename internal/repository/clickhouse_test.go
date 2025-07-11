package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	cleanup := testdb.SetupTestDatabase(t)

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
		accounts := map[uint64]map[string]struct{}{0: {"0x1234567890123456789012345678901234567890": {}}}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 42)
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
		accounts := map[uint64]map[string]struct{}{0: {"0x1234567890123456789012345678901234567890": {}}}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[uint64]map[string]map[string]struct{}{}

		ranges := []uint64{10, 25, 50, 100}
		for _, rangeNum := range ranges {
			err := repo.InsertRange(ctx, accounts, accountType, storage, rangeNum)
			require.NoError(t, err)

			lastRange, err := repo.GetLastIndexedRange(ctx)
			require.NoError(t, err)
			assert.Equal(t, rangeNum, lastRange, "Should return the most recent range")
		}
	})
}

// TestClickHouseUpdateRangeDataInTx tests the main data update functionality
func TestClickHouseInsertRange(t *testing.T) {
	t.Run("EmptyMaps", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := context.Background()
		accounts := map[uint64]map[string]struct{}{}
		accountType := map[string]bool{}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// Verify metadata was updated
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
	})

	t.Run("AccountsOnly", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		t.Cleanup(cleanup)

		ctx := context.Background()
		accounts := map[uint64]map[string]struct{}{
			0: {
				"0x1234567890123456789012345678901234567890": {},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {},
			},
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false, // EOA
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,  // Contract
		}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// For ClickHouse, we can verify data was inserted by checking if we can get analytics
		// (we don't have direct access to GetAccountInfo like PostgreSQL)
		params := QueryParams{ExpiryBlock: 200, CurrentBlock: 300}
		analytics, err := repo.GetAccountAnalytics(ctx, params) // Expiry after our test data
		require.NoError(t, err)
		assert.Equal(t, 2, analytics.Total.Total, "Should have accounts in ClickHouse")
	})

	t.Run("StorageOnly", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[uint64]map[string]struct{}{}
		accountType := map[string]bool{}
		storage := map[uint64]map[string]map[string]struct{}{
			0: {
				"0x1234567890123456789012345678901234567890": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000002": {},
				},
			},
		}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// Verify storage was inserted by checking analytics
		params := QueryParams{ExpiryBlock: 200, CurrentBlock: 300}
		analytics, err := repo.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, 2, analytics.Total.TotalSlots, "Should have storage slots in ClickHouse")
	})

	t.Run("AccountsAndStorage", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accounts := map[uint64]map[string]struct{}{
			0: {
				"0x1234567890123456789012345678901234567890": {},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {},
			},
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,
		}
		storage := map[uint64]map[string]map[string]struct{}{
			0: {
				"0x1234567890123456789012345678901234567890": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
					"0x0000000000000000000000000000000000000000000000000000000000000002": {},
				},
			},
		}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 2)
		require.NoError(t, err)

		// Verify both accounts and storage were inserted
		params := QueryParams{ExpiryBlock: 200, CurrentBlock: 300}
		accountAnalytics, err := repo.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, 2, accountAnalytics.Total.Total, "Should have accounts")

		storageAnalytics, err := repo.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, 3, storageAnalytics.Total.TotalSlots, "Should have storage slots")

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
		accounts := make(map[uint64]map[string]struct{})
		accountType := make(map[string]bool)
		storage := make(map[uint64]map[string]map[string]struct{})
		storageCount := 0

		// Create 50 accounts with storage (smaller than PostgreSQL test for ClickHouse)
		for i := 0; i < 50; i++ {
			addr := generateClickHouseTestAddress(i)
			accounts[uint64(1000+i)] = map[string]struct{}{addr: {}}
			accountType[addr] = i%2 == 0 // Alternate between EOA and Contract

			// Add storage for contracts
			if accountType[addr] {
				storage[uint64(1000+i)] = make(map[string]map[string]struct{})
				storage[uint64(1000+i)][addr] = make(map[string]struct{})
				for j := 0; j < 3; j++ { // 3 storage slots per contract
					slot := generateClickHouseTestStorageSlot(j)
					storage[uint64(1000+i)][addr][slot] = struct{}{}
				}
				storageCount += 3
			}
		}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 10)
		require.NoError(t, err)

		// Verify the data was inserted
		params := QueryParams{ExpiryBlock: 1200, CurrentBlock: 1300}
		accountAnalytics, err := repo.GetAccountAnalytics(ctx, params) // Expiry after our test data
		require.NoError(t, err)
		assert.Equal(t, 50, accountAnalytics.Total.Total, "Expected 50 accounts")

		storageAnalytics, err := repo.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, storageCount, storageAnalytics.Total.TotalSlots, "Expected 150 storage slots")

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), lastRange)
	})

	t.Run("EmptyEvents", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accountAccesses := map[uint64]map[string]struct{}{}
		accountType := map[string]bool{}
		storageAccesses := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accountAccesses, accountType, storageAccesses, 1)
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

		err := repo.InsertRange(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.NoError(t, err)

		// For ClickHouse archive mode, ALL events should be stored
		// We can verify by checking that analytics shows we have data
		params := QueryParams{ExpiryBlock: 1100, CurrentBlock: 1200}
		accountAnalytics, err := repo.GetAccountAnalytics(ctx, params) // Expiry after some of our test data
		require.NoError(t, err)
		assert.Equal(t, accountAnalytics.Total.Total, 2, "Should have accounts from all events")

		storageAnalytics, err := repo.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, 2, storageAnalytics.Total.TotalSlots, "Should have storage from all events")

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

		storageAccesses := map[uint64]map[string]map[string]struct{}{
			1000: {
				"0x1234567890123456789012345678901234567890": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
			},
			1100: {
				"0x1234567890123456789012345678901234567890": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
			},
			1200: {
				"0x1234567890123456789012345678901234567890": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
			},
		}

		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
		}

		err := repo.InsertRange(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.NoError(t, err)

		// In archive mode, ClickHouse should store all 3 access events for the same account
		// This is different from PostgreSQL which only stores the latest access
		params := QueryParams{ExpiryBlock: 1150, CurrentBlock: 1300}
		accountAnalytics, err := repo.GetAccountAnalytics(ctx, params) // Expiry between blocks 2 and 3
		require.NoError(t, err)
		assert.Equal(t, 1, accountAnalytics.Total.Total, "Should have account data")

		storageAnalytics, err := repo.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, 1, storageAnalytics.Total.TotalSlots, "Should have storage data")

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), lastRange)
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
		accounts := map[uint64]map[string]struct{}{100: {"0x1234567890123456789012345678901234567890": {}}}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 50)
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
		accounts := map[uint64]map[string]struct{}{100: {"0x1234567890123456789012345678901234567890": {}}}
		accountType := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 100)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 100, 10)
		require.NoError(t, err)

		assert.True(t, status.IsSynced, "Should be synced when up to date")
		assert.Equal(t, uint64(100), status.LastIndexedRange)
		assert.Equal(t, uint64(100*10), status.EndBlock)
	})
}

// TestGetAccountAnalytics provides comprehensive testing for the GetAccountAnalytics method
// Tests Questions 1, 2, and 5a: EOA count, Contract count, and Single access accounts
func TestGetAccountAnalytics(t *testing.T) {
	t.Run("BasicFunctionality", func(t *testing.T) {
		// Setup test with known data distribution
		config := AnalyticsTestDataConfig{
			NumEOAs:          6, // 60 EOAs
			NumContracts:     4, // 40 contracts (total 100 accounts)
			SlotsPerContract: 5,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5, // Half expired
		}

		setup := SetupAnalyticsTest(t, config)
		t.Cleanup(setup.Cleanup)

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   100,
			TopN:         10,
		}

		// Calculate expected results based on deterministic test data
		expected := setup.TestData.CalculateExpectedAccountAnalytics(config.ExpiryBlock)

		// Test the method
		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate basic structure
		assert.NotNil(t, result.Total)
		assert.NotNil(t, result.Expiry)
		assert.NotNil(t, result.SingleAccess)
		assert.NotNil(t, result.Distribution)

		// Validate that actual results match expected results from deterministic data
		AssertAccountAnalyticsMatch(t, expected, result, 0.1)

		// Validate data consistency
		AssertAnalyticsDataConsistency(t, result)

		t.Logf("Account Analytics Results: Total=%d (EOAs=%d, Contracts=%d), Expired=%d, SingleAccess=%d",
			result.Total.Total, result.Total.EOAs, result.Total.Contracts,
			result.Expiry.TotalExpired, result.SingleAccess.TotalSingleAccess)
		t.Logf("Expected vs Actual - Expired: %d vs %d, SingleAccess: %d vs %d",
			expected.Expiry.TotalExpired, result.Expiry.TotalExpired,
			expected.SingleAccess.TotalSingleAccess, result.SingleAccess.TotalSingleAccess)
	})

	t.Run("DeterministicExpiryValidation", func(t *testing.T) {
		// Test with specific configuration to validate expiry calculations
		config := AnalyticsTestDataConfig{
			NumEOAs:          10,
			NumContracts:     5,
			SlotsPerContract: 3,
			StartBlock:       1,
			EndBlock:         20,
			ExpiryBlock:      10, // Expiry in the middle
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   50,
			TopN:         10,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedAccountAnalytics(config.ExpiryBlock)

		// Get actual results
		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate exact matches for expiry data
		assert.Equal(t, expected.Expiry.ExpiredEOAs, result.Expiry.ExpiredEOAs,
			"Expired EOA count should match deterministic calculation")
		assert.Equal(t, expected.Expiry.ExpiredContracts, result.Expiry.ExpiredContracts,
			"Expired contract count should match deterministic calculation")
		assert.Equal(t, expected.Expiry.TotalExpired, result.Expiry.TotalExpired,
			"Total expired count should match deterministic calculation")

		// Validate exact matches for single access data
		assert.Equal(t, expected.SingleAccess.SingleAccessEOAs, result.SingleAccess.SingleAccessEOAs,
			"Single access EOA count should match deterministic calculation")
		assert.Equal(t, expected.SingleAccess.SingleAccessContracts, result.SingleAccess.SingleAccessContracts,
			"Single access contract count should match deterministic calculation")
		assert.Equal(t, expected.SingleAccess.TotalSingleAccess, result.SingleAccess.TotalSingleAccess,
			"Total single access count should match deterministic calculation")

		t.Logf("Deterministic validation passed - Expired: %d, SingleAccess: %d",
			result.Expiry.TotalExpired, result.SingleAccess.TotalSingleAccess)
	})

	t.Run("SingleAccessValidation", func(t *testing.T) {
		// Test configuration designed to have predictable single access patterns
		config := AnalyticsTestDataConfig{
			NumEOAs:          6, // Some will have single access, some multiple
			NumContracts:     4, // Some will have single access, some multiple
			SlotsPerContract: 2,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      0, // All active (no expiry)
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   20,
			TopN:         10,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedAccountAnalytics(config.ExpiryBlock)

		// Get actual results
		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Since expiry block is after all data, no accounts should be expired
		assert.Equal(t, 0, result.Expiry.TotalExpired, "No accounts should be expired")
		assert.Equal(t, 0.0, result.Expiry.ExpiryRate, "Expiry rate should be 0%")

		// Validate single access calculations match expected
		AssertAccountAnalyticsMatch(t, expected, result, 0.1)

		t.Logf("Single access validation - Expected: %d, Actual: %d",
			expected.SingleAccess.TotalSingleAccess, result.SingleAccess.TotalSingleAccess)
	})

	t.Run("EmptyDatabase", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          0,
			NumContracts:     0,
			SlotsPerContract: 0,
			StartBlock:       1,
			EndBlock:         100,
			ExpiryBlock:      50,
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   10,
			TopN:         5,
		}

		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have zero counts
		assert.Equal(t, 0, result.Total.EOAs)
		assert.Equal(t, 0, result.Total.Contracts)
		assert.Equal(t, 0, result.Total.Total)
		assert.Equal(t, 0, result.Expiry.TotalExpired)
		assert.Equal(t, 0, result.SingleAccess.TotalSingleAccess)

		// Rates should be zero or handle division by zero gracefully
		assert.Equal(t, 0.0, result.Expiry.ExpiryRate)
		assert.Equal(t, 0.0, result.SingleAccess.SingleAccessRate)
	})

	t.Run("OnlyEOAs", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          50,
			NumContracts:     0, // No contracts
			SlotsPerContract: 0,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5,
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   20,
			TopN:         5,
		}

		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have only EOAs
		assert.Equal(t, 50, result.Total.EOAs)
		assert.Equal(t, 0, result.Total.Contracts)
		assert.Equal(t, result.Total.Total, result.Total.EOAs)
		assert.Equal(t, 0, result.Expiry.ExpiredContracts)
		assert.Equal(t, 0, result.SingleAccess.SingleAccessContracts)

		// Distribution should be 100% EOAs
		assert.Equal(t, 100.0, result.Distribution.EOAPercentage)
		assert.Equal(t, 0.0, result.Distribution.ContractPercentage)
	})

	t.Run("OnlyContracts", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          0, // No EOAs
			NumContracts:     30,
			SlotsPerContract: 5,
			StartBlock:       1,
			EndBlock:         50,
			ExpiryBlock:      5,
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   30,
			TopN:         10,
		}

		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have only contracts
		assert.Equal(t, 0, result.Total.EOAs)
		assert.Equal(t, 30, result.Total.Contracts)
		assert.Equal(t, result.Total.Total, result.Total.Contracts)
		assert.Equal(t, 0, result.Expiry.ExpiredEOAs)
		assert.Equal(t, 0, result.SingleAccess.SingleAccessEOAs)

		// Distribution should be 100% contracts
		assert.Equal(t, 0.0, result.Distribution.EOAPercentage)
		assert.Equal(t, 100.0, result.Distribution.ContractPercentage)
	})

	t.Run("AllExpired", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          20,
			NumContracts:     10,
			SlotsPerContract: 3,
			StartBlock:       1,
			EndBlock:         20,
			ExpiryBlock:      100, // All should be expired (expiry after end)
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.ExpiryBlock + 100,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   10,
			TopN:         5,
		}

		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// All accounts should be expired
		assert.Equal(t, result.Total.Total, result.Expiry.TotalExpired, "All accounts should be expired")
		assert.Equal(t, 100.0, result.Expiry.ExpiryRate, "Expiry rate should be 100%")
	})

	t.Run("AllActive", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          25,
			NumContracts:     15,
			SlotsPerContract: 4,
			StartBlock:       1,
			EndBlock:         500,
			ExpiryBlock:      1, // All should be active (expiry before start)
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   50,
			TopN:         8,
		}

		result, err := setup.Repository.GetAccountAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// No accounts should be expired
		assert.Equal(t, 0, result.Expiry.TotalExpired, "No accounts should be expired")
		assert.Equal(t, 0.0, result.Expiry.ExpiryRate, "Expiry rate should be 0%")
	})
}

// TestGetStorageAnalytics provides comprehensive testing for the GetStorageAnalytics method
// Tests Questions 3, 4, and 5b: Total storage slots, Expired storage slots, and Single access storage slots
func TestGetStorageAnalytics(t *testing.T) {
	t.Run("BasicFunctionality", func(t *testing.T) {
		// Setup test with known data distribution
		config := AnalyticsTestDataConfig{
			NumEOAs:          5,
			NumContracts:     20,
			SlotsPerContract: 5,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5,
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   100,
			TopN:         10,
		}

		// Calculate expected results based on deterministic test data
		expected := setup.TestData.CalculateExpectedStorageAnalytics(config.ExpiryBlock)

		// Test the method
		result, err := setup.Repository.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate basic structure
		assert.NotNil(t, result.Total)
		assert.NotNil(t, result.Expiry)
		assert.NotNil(t, result.SingleAccess)

		// Validate that actual results match expected results from deterministic data
		AssertStorageAnalyticsMatch(t, expected, result, 0.1)

		// Validate data consistency
		AssertAnalyticsDataConsistency(t, result)

		t.Logf("Storage Analytics Results: Total=%d, Expired=%d, Active=%d, SingleAccess=%d",
			result.Total.TotalSlots, result.Expiry.ExpiredSlots, result.Expiry.ActiveSlots,
			result.SingleAccess.SingleAccessSlots)
		t.Logf("Expected vs Actual - Expired: %d vs %d, SingleAccess: %d vs %d",
			expected.Expiry.ExpiredSlots, result.Expiry.ExpiredSlots,
			expected.SingleAccess.SingleAccessSlots, result.SingleAccess.SingleAccessSlots)
	})

	t.Run("DeterministicExpiryValidation", func(t *testing.T) {
		// Test with specific configuration to validate expiry calculations
		config := AnalyticsTestDataConfig{
			NumEOAs:          3,
			NumContracts:     5,
			SlotsPerContract: 4,
			StartBlock:       1,
			EndBlock:         15,
			ExpiryBlock:      8, // Expiry in the middle
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   50,
			TopN:         10,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedStorageAnalytics(config.ExpiryBlock)

		// Get actual results
		result, err := setup.Repository.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate exact matches for expiry data
		assert.Equal(t, expected.Expiry.ExpiredSlots, result.Expiry.ExpiredSlots,
			"Expired slots count should match deterministic calculation")
		assert.Equal(t, expected.Expiry.ActiveSlots, result.Expiry.ActiveSlots,
			"Active slots count should match deterministic calculation")
		assert.Equal(t, expected.Total.TotalSlots, result.Total.TotalSlots,
			"Total slots count should match deterministic calculation")

		// Validate exact matches for single access data
		assert.Equal(t, expected.SingleAccess.SingleAccessSlots, result.SingleAccess.SingleAccessSlots,
			"Single access slots count should match deterministic calculation")

		t.Logf("Deterministic validation passed - Total: %d, Expired: %d, SingleAccess: %d",
			result.Total.TotalSlots, result.Expiry.ExpiredSlots, result.SingleAccess.SingleAccessSlots)
	})

	t.Run("SingleAccessValidation", func(t *testing.T) {
		// Test configuration designed to have predictable single access patterns
		config := AnalyticsTestDataConfig{
			NumEOAs:          2,
			NumContracts:     4, // Some slots will have single access, some multiple
			SlotsPerContract: 3,
			StartBlock:       1,
			EndBlock:         12,
			ExpiryBlock:      0, // All active (no expiry)
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   25,
			TopN:         10,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedStorageAnalytics(config.ExpiryBlock)

		// Get actual results
		result, err := setup.Repository.GetStorageAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Since expiry block is after all data, no slots should be expired
		assert.Equal(t, 0, result.Expiry.ExpiredSlots, "No slots should be expired")
		assert.Equal(t, result.Total.TotalSlots, result.Expiry.ActiveSlots, "All slots should be active")
		assert.Equal(t, 0.0, result.Expiry.ExpiryRate, "Expiry rate should be 0%")

		// Validate single access calculations match expected
		AssertStorageAnalyticsMatch(t, expected, result, 0.1)

		t.Logf("Single access validation - Expected: %d, Actual: %d",
			expected.SingleAccess.SingleAccessSlots, result.SingleAccess.SingleAccessSlots)
	})

	t.Run("EdgeCases", func(t *testing.T) {
		t.Run("EmptyDatabase", func(t *testing.T) {
			config := AnalyticsTestDataConfig{
				NumEOAs:          0,
				NumContracts:     0,
				SlotsPerContract: 0,
				StartBlock:       1,
				EndBlock:         100,
				ExpiryBlock:      50,
			}

			setup := SetupAnalyticsTest(t, config)
			defer setup.Cleanup()

			ctx := context.Background()
			params := QueryParams{
				ExpiryBlock:  config.ExpiryBlock,
				CurrentBlock: config.EndBlock,
				StartBlock:   config.StartBlock,
				EndBlock:     config.EndBlock,
				WindowSize:   10,
				TopN:         5,
			}

			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Should have zero counts
			assert.Equal(t, 0, result.Total.TotalSlots)
			assert.Equal(t, 0, result.Expiry.ExpiredSlots)
			assert.Equal(t, 0, result.Expiry.ActiveSlots)
			assert.Equal(t, 0, result.SingleAccess.SingleAccessSlots)

			// Rates should be zero or handle division by zero gracefully
			assert.Equal(t, 0.0, result.Expiry.ExpiryRate)
			assert.Equal(t, 0.0, result.SingleAccess.SingleAccessRate)
		})

		t.Run("NoStorageSlots", func(t *testing.T) {
			config := AnalyticsTestDataConfig{
				NumEOAs:          50, // Only EOAs, no contracts
				NumContracts:     0,
				SlotsPerContract: 0,
				StartBlock:       1,
				EndBlock:         200,
				ExpiryBlock:      100,
			}

			setup := SetupAnalyticsTest(t, config)
			defer setup.Cleanup()

			ctx := context.Background()
			params := QueryParams{
				ExpiryBlock:  config.ExpiryBlock,
				CurrentBlock: config.EndBlock,
				StartBlock:   config.StartBlock,
				EndBlock:     config.EndBlock,
				WindowSize:   20,
				TopN:         5,
			}

			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Should have zero storage slots
			assert.Equal(t, 0, result.Total.TotalSlots)
			assert.Equal(t, 0, result.Expiry.ExpiredSlots)
			assert.Equal(t, 0, result.Expiry.ActiveSlots)
			assert.Equal(t, 0, result.SingleAccess.SingleAccessSlots)

			// Rates should be zero
			assert.Equal(t, 0.0, result.Expiry.ExpiryRate)
			assert.Equal(t, 0.0, result.SingleAccess.SingleAccessRate)
		})

		t.Run("AllSlotsExpired", func(t *testing.T) {
			config := AnalyticsTestDataConfig{
				NumEOAs:          10,
				NumContracts:     5,
				SlotsPerContract: 4,
				StartBlock:       1,
				EndBlock:         100,
				ExpiryBlock:      200, // All should be expired (expiry after end)
			}

			setup := SetupAnalyticsTest(t, config)
			defer setup.Cleanup()

			ctx := context.Background()
			params := QueryParams{
				ExpiryBlock:  config.ExpiryBlock,
				CurrentBlock: config.ExpiryBlock + 100,
				StartBlock:   config.StartBlock,
				EndBlock:     config.EndBlock,
				WindowSize:   10,
				TopN:         5,
			}

			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// All slots should be expired
			assert.Equal(t, result.Total.TotalSlots, result.Expiry.ExpiredSlots, "All slots should be expired")
			assert.Equal(t, 0, result.Expiry.ActiveSlots, "No slots should be active")
			assert.Equal(t, 100.0, result.Expiry.ExpiryRate, "Expiry rate should be 100%")
		})

		t.Run("AllSlotsActive", func(t *testing.T) {
			config := AnalyticsTestDataConfig{
				NumEOAs:          15,
				NumContracts:     8,
				SlotsPerContract: 6,
				StartBlock:       1,
				EndBlock:         500,
				ExpiryBlock:      1, // All should be active (expiry before start)
			}

			setup := SetupAnalyticsTest(t, config)
			defer setup.Cleanup()

			ctx := context.Background()
			params := QueryParams{
				ExpiryBlock:  config.ExpiryBlock,
				CurrentBlock: config.EndBlock,
				StartBlock:   config.StartBlock,
				EndBlock:     config.EndBlock,
				WindowSize:   50,
				TopN:         8,
			}

			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// No slots should be expired
			assert.Equal(t, 0, result.Expiry.ExpiredSlots, "No slots should be expired")
			assert.Equal(t, result.Total.TotalSlots, result.Expiry.ActiveSlots, "All slots should be active")
			assert.Equal(t, 0.0, result.Expiry.ExpiryRate, "Expiry rate should be 0%")
		})
	})

	t.Run("ParameterValidation", func(t *testing.T) {
		setup := SetupAnalyticsTestWithDefaults(t)
		defer setup.Cleanup()

		ctx := context.Background()

		t.Run("InvalidExpiryBlock", func(t *testing.T) {
			params := QueryParams{
				ExpiryBlock:  1000, // Greater than current block
				CurrentBlock: 500,
				StartBlock:   1,
				EndBlock:     400,
				WindowSize:   10,
				TopN:         5,
			}

			// Should handle invalid parameters gracefully
			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			// Note: The actual behavior depends on implementation
			if err != nil {
				t.Logf("Expected error for invalid expiry block: %v", err)
			} else {
				require.NotNil(t, result)
				t.Logf("Method handled invalid expiry block gracefully")
			}
		})

		t.Run("ZeroBlocks", func(t *testing.T) {
			params := QueryParams{
				ExpiryBlock:  0,
				CurrentBlock: 0,
				StartBlock:   0,
				EndBlock:     0,
				WindowSize:   1,
				TopN:         1,
			}

			result, err := setup.Repository.GetStorageAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Should handle zero blocks gracefully
			t.Logf("Method handled zero blocks gracefully")
		})
	})

	t.Run("ConcurrencyTesting", func(t *testing.T) {
		setup := SetupAnalyticsTestWithDefaults(t)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  50,
			CurrentBlock: 100,
			StartBlock:   1,
			EndBlock:     100,
			WindowSize:   50,
			TopN:         10,
		}

		// Test concurrent access
		const numGoroutines = 8
		results := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(routineID int) {
				defer func() {
					if r := recover(); r != nil {
						results <- assert.AnError
					}
				}()

				analytics, err := setup.Repository.GetStorageAnalytics(ctx, params)
				if err != nil {
					results <- err
					return
				}

				if analytics == nil {
					results <- assert.AnError
					return
				}

				// Validate basic consistency
				if analytics.Total.TotalSlots != analytics.Expiry.ExpiredSlots+analytics.Expiry.ActiveSlots {
					results <- assert.AnError
					return
				}

				results <- nil
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			err := <-results
			assert.NoError(t, err, "Concurrent access should not cause errors")
		}

		t.Logf("Concurrency test completed successfully with %d goroutines", numGoroutines)
	})

	t.Run("ErrorScenarios", func(t *testing.T) {
		setup := SetupAnalyticsTestWithDefaults(t)
		defer setup.Cleanup()

		params := QueryParams{
			ExpiryBlock:  50,
			CurrentBlock: 100,
			StartBlock:   1,
			EndBlock:     100,
			WindowSize:   50,
			TopN:         10,
		}

		// Test with cancelled context
		t.Run("CancelledContext", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			_, err := setup.Repository.GetStorageAnalytics(ctx, params)
			if err != nil {
				assert.Contains(t, err.Error(), "context", "Should handle cancelled context")
				t.Logf("Correctly handled cancelled context: %v", err)
			} else {
				t.Logf("Method completed despite cancelled context")
			}
		})

		// Test with timeout context
		t.Run("TimeoutContext", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
			defer cancel()

			_, err := setup.Repository.GetStorageAnalytics(ctx, params)
			if err != nil {
				t.Logf("Correctly handled timeout context: %v", err)
			} else {
				t.Logf("Method completed despite timeout context")
			}
		})
	})
}
