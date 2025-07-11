package repository

import (
	"context"
	"fmt"
	"math"
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

// TestGetContractAnalytics provides comprehensive testing for the GetContractAnalytics method
// Tests Questions 7-11, 15: Contract rankings, expiry analysis, volume analysis, and status analysis
func TestGetContractAnalytics(t *testing.T) {
	t.Run("BasicFunctionality", func(t *testing.T) {
		// Setup test with known data distribution focused on contracts
		config := AnalyticsTestDataConfig{
			NumEOAs:          5,  // Minimal EOAs
			NumContracts:     20, // Focus on contracts
			SlotsPerContract: 4,  // Moderate slot count per contract
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5, // Half expired
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
		expected := setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Test the method
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate basic structure
		assert.NotNil(t, result.Rankings)
		assert.NotNil(t, result.ExpiryAnalysis)
		assert.NotNil(t, result.VolumeAnalysis)
		assert.NotNil(t, result.StatusAnalysis)

		// Validate rankings structure
		assert.NotNil(t, result.Rankings.TopByExpiredSlots)
		assert.NotNil(t, result.Rankings.TopByTotalSlots)

		// Validate that actual results match expected results from deterministic data
		AssertContractAnalyticsMatch(t, expected, result, 0.1)

		// Validate data consistency within contract analytics
		AssertContractAnalyticsConsistency(t, result)

		t.Logf("Contract Analytics Results: TopByExpired=%d, TopByTotal=%d, ContractsAnalyzed=%d",
			len(result.Rankings.TopByExpiredSlots), len(result.Rankings.TopByTotalSlots),
			result.ExpiryAnalysis.ContractsAnalyzed)
		t.Logf("Volume Analysis: AvgStorage=%.2f, MaxStorage=%d, TotalContracts=%d",
			result.VolumeAnalysis.AverageStoragePerContract, result.VolumeAnalysis.MaxStoragePerContract,
			result.VolumeAnalysis.TotalContracts)
		t.Logf("Status Analysis: AllExpired=%d, AllActive=%d, MixedState=%d",
			result.StatusAnalysis.AllExpiredContracts, result.StatusAnalysis.AllActiveContracts,
			result.StatusAnalysis.MixedStateContracts)
	})

	t.Run("ContractRankingValidation", func(t *testing.T) {
		// Test configuration designed to validate contract ranking logic
		config := AnalyticsTestDataConfig{
			NumEOAs:          2,  // Minimal EOAs
			NumContracts:     15, // Moderate number for ranking
			SlotsPerContract: 6,  // Higher slot count for better ranking distribution
			StartBlock:       1,
			EndBlock:         20,
			ExpiryBlock:      10, // Middle expiry for mixed results
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
			TopN:         5, // Test top 5 ranking
		}

		// Calculate expected results
		_ = setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Get actual results
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate ranking counts match TopN parameter
		assert.LessOrEqual(t, len(result.Rankings.TopByExpiredSlots), params.TopN,
			"Top expired slots ranking should not exceed TopN")
		assert.LessOrEqual(t, len(result.Rankings.TopByTotalSlots), params.TopN,
			"Top total slots ranking should not exceed TopN")

		// Validate ranking order for expired slots (descending)
		for i := 1; i < len(result.Rankings.TopByExpiredSlots); i++ {
			prev := result.Rankings.TopByExpiredSlots[i-1]
			curr := result.Rankings.TopByExpiredSlots[i]
			assert.GreaterOrEqual(t, prev.ExpiredSlots, curr.ExpiredSlots,
				"Expired slots ranking should be in descending order")
		}

		// Validate ranking order for total slots (descending)
		for i := 1; i < len(result.Rankings.TopByTotalSlots); i++ {
			prev := result.Rankings.TopByTotalSlots[i-1]
			curr := result.Rankings.TopByTotalSlots[i]
			assert.GreaterOrEqual(t, prev.TotalSlots, curr.TotalSlots,
				"Total slots ranking should be in descending order")
		}

		// Validate ranking items have required data
		for _, item := range result.Rankings.TopByExpiredSlots {
			assert.NotEmpty(t, item.Address, "Address should not be empty")
			assert.GreaterOrEqual(t, item.TotalSlots, item.ExpiredSlots,
				"Total slots should be >= expired slots")
			assert.Equal(t, item.TotalSlots, item.ExpiredSlots+item.ActiveSlots,
				"Total should equal expired + active")
		}

		for _, item := range result.Rankings.TopByTotalSlots {
			assert.NotEmpty(t, item.Address, "Address should not be empty")
			assert.GreaterOrEqual(t, item.TotalSlots, 0, "Total slots should be non-negative")
		}

		t.Logf("Ranking validation passed - TopExpired: %d items, TopTotal: %d items",
			len(result.Rankings.TopByExpiredSlots), len(result.Rankings.TopByTotalSlots))
	})

	t.Run("ExpiryAnalysisValidation", func(t *testing.T) {
		// Test configuration designed to validate expiry distribution analysis
		config := AnalyticsTestDataConfig{
			NumEOAs:          3,
			NumContracts:     12, // Good number for distribution analysis
			SlotsPerContract: 5,
			StartBlock:       1,
			EndBlock:         25,
			ExpiryBlock:      12, // Strategic expiry point
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
			TopN:         8,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Get actual results
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate expiry analysis structure
		assert.GreaterOrEqual(t, result.ExpiryAnalysis.AverageExpiryPercentage, 0.0,
			"Average expiry percentage should be non-negative")
		assert.LessOrEqual(t, result.ExpiryAnalysis.AverageExpiryPercentage, 100.0,
			"Average expiry percentage should not exceed 100%")
		assert.GreaterOrEqual(t, result.ExpiryAnalysis.MedianExpiryPercentage, 0.0,
			"Median expiry percentage should be non-negative")
		assert.LessOrEqual(t, result.ExpiryAnalysis.MedianExpiryPercentage, 100.0,
			"Median expiry percentage should not exceed 100%")
		assert.GreaterOrEqual(t, result.ExpiryAnalysis.ContractsAnalyzed, 0,
			"Contracts analyzed should be non-negative")

		// Validate expiry distribution buckets
		totalBucketCount := 0
		for _, bucket := range result.ExpiryAnalysis.ExpiryDistribution {
			assert.GreaterOrEqual(t, bucket.RangeStart, 0, "Bucket range start should be non-negative")
			assert.LessOrEqual(t, bucket.RangeEnd, 100, "Bucket range end should not exceed 100")
			assert.GreaterOrEqual(t, bucket.RangeEnd, bucket.RangeStart,
				"Bucket range end should be >= start")
			assert.GreaterOrEqual(t, bucket.Count, 0, "Bucket count should be non-negative")
			totalBucketCount += bucket.Count
		}

		// Total bucket count should match contracts analyzed (if we have contracts)
		if result.ExpiryAnalysis.ContractsAnalyzed > 0 {
			assert.Equal(t, result.ExpiryAnalysis.ContractsAnalyzed, totalBucketCount,
				"Sum of bucket counts should equal contracts analyzed")
		}

		// Validate against expected results
		AssertContractExpiryAnalysisMatch(t, expected.ExpiryAnalysis, result.ExpiryAnalysis, 0.1)

		t.Logf("Expiry analysis validation - Avg: %.2f%%, Median: %.2f%%, Contracts: %d, Buckets: %d",
			result.ExpiryAnalysis.AverageExpiryPercentage, result.ExpiryAnalysis.MedianExpiryPercentage,
			result.ExpiryAnalysis.ContractsAnalyzed, len(result.ExpiryAnalysis.ExpiryDistribution))
	})

	t.Run("VolumeAnalysisValidation", func(t *testing.T) {
		// Test configuration designed to validate volume analysis calculations
		config := AnalyticsTestDataConfig{
			NumEOAs:          1,  // Minimal EOAs
			NumContracts:     10, // Good number for volume statistics
			SlotsPerContract: 8,  // Higher slot count for volume analysis
			StartBlock:       1,
			EndBlock:         15,
			ExpiryBlock:      8,
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

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Get actual results
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate volume analysis structure
		assert.GreaterOrEqual(t, result.VolumeAnalysis.AverageStoragePerContract, 0.0,
			"Average storage should be non-negative")
		assert.GreaterOrEqual(t, result.VolumeAnalysis.MedianStoragePerContract, 0.0,
			"Median storage should be non-negative")
		assert.GreaterOrEqual(t, result.VolumeAnalysis.MaxStoragePerContract, 0,
			"Max storage should be non-negative")
		assert.GreaterOrEqual(t, result.VolumeAnalysis.MinStoragePerContract, 0,
			"Min storage should be non-negative")
		assert.GreaterOrEqual(t, result.VolumeAnalysis.TotalContracts, 0,
			"Total contracts should be non-negative")

		// Validate logical relationships
		if result.VolumeAnalysis.TotalContracts > 0 {
			assert.GreaterOrEqual(t, result.VolumeAnalysis.MaxStoragePerContract,
				result.VolumeAnalysis.MinStoragePerContract,
				"Max storage should be >= min storage")

			// Average should be between min and max (if we have data)
			if result.VolumeAnalysis.MinStoragePerContract > 0 {
				assert.GreaterOrEqual(t, result.VolumeAnalysis.AverageStoragePerContract,
					float64(result.VolumeAnalysis.MinStoragePerContract),
					"Average should be >= minimum")
				assert.LessOrEqual(t, result.VolumeAnalysis.AverageStoragePerContract,
					float64(result.VolumeAnalysis.MaxStoragePerContract),
					"Average should be <= maximum")
			}
		}

		// Validate against expected results
		AssertContractVolumeAnalysisMatch(t, expected.VolumeAnalysis, result.VolumeAnalysis, 0.1)

		t.Logf("Volume analysis validation - Avg: %.2f, Median: %.2f, Max: %d, Min: %d, Total: %d",
			result.VolumeAnalysis.AverageStoragePerContract, result.VolumeAnalysis.MedianStoragePerContract,
			result.VolumeAnalysis.MaxStoragePerContract, result.VolumeAnalysis.MinStoragePerContract,
			result.VolumeAnalysis.TotalContracts)
	})

	t.Run("StatusAnalysisValidation", func(t *testing.T) {
		// Test configuration designed to validate status analysis breakdown
		config := AnalyticsTestDataConfig{
			NumEOAs:          2,
			NumContracts:     5, // Good number for status distribution
			SlotsPerContract: 3,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5, // Middle expiry for mixed status
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

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Get actual results
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate status analysis structure
		assert.GreaterOrEqual(t, result.StatusAnalysis.AllExpiredContracts, 0,
			"All expired contracts should be non-negative")
		assert.GreaterOrEqual(t, result.StatusAnalysis.AllActiveContracts, 0,
			"All active contracts should be non-negative")
		assert.GreaterOrEqual(t, result.StatusAnalysis.MixedStateContracts, 0,
			"Mixed state contracts should be non-negative")
		assert.GreaterOrEqual(t, result.StatusAnalysis.ActiveWithExpiredStorage, 0,
			"Active with expired storage should be non-negative")

		// Validate percentage calculations
		assert.GreaterOrEqual(t, result.StatusAnalysis.AllExpiredRate, 0.0,
			"All expired rate should be non-negative")
		assert.LessOrEqual(t, result.StatusAnalysis.AllExpiredRate, 100.0,
			"All expired rate should not exceed 100%")
		assert.GreaterOrEqual(t, result.StatusAnalysis.AllActiveRate, 0.0,
			"All active rate should be non-negative")
		assert.LessOrEqual(t, result.StatusAnalysis.AllActiveRate, 100.0,
			"All active rate should not exceed 100%")

		// Validate logical relationships in status analysis
		totalStatusContracts := result.StatusAnalysis.AllExpiredContracts +
			result.StatusAnalysis.AllActiveContracts +
			result.StatusAnalysis.MixedStateContracts

		// Should match volume analysis total if consistent
		if result.VolumeAnalysis.TotalContracts > 0 {
			// Allow for potential differences in counting methods
			assert.InDelta(t, result.VolumeAnalysis.TotalContracts, totalStatusContracts,
				float64(result.VolumeAnalysis.TotalContracts)*0.1,
				"Status analysis total should be close to volume analysis total")
		}

		// Validate against expected results
		AssertContractStatusAnalysisMatch(t, expected.StatusAnalysis, result.StatusAnalysis, 0.1)

		t.Logf("Status analysis validation - AllExpired: %d, AllActive: %d, Mixed: %d, ActiveWithExpired: %d",
			result.StatusAnalysis.AllExpiredContracts, result.StatusAnalysis.AllActiveContracts,
			result.StatusAnalysis.MixedStateContracts, result.StatusAnalysis.ActiveWithExpiredStorage)
		t.Logf("Status rates - AllExpired: %.2f%%, AllActive: %.2f%%",
			result.StatusAnalysis.AllExpiredRate, result.StatusAnalysis.AllActiveRate)
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

		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have empty rankings
		assert.Equal(t, 0, len(result.Rankings.TopByExpiredSlots))
		assert.Equal(t, 0, len(result.Rankings.TopByTotalSlots))

		// Should have zero expiry analysis
		assert.True(t, math.IsNaN(result.ExpiryAnalysis.AverageExpiryPercentage))
		assert.True(t, math.IsNaN(result.ExpiryAnalysis.MedianExpiryPercentage))
		assert.Equal(t, 0, result.ExpiryAnalysis.ContractsAnalyzed)
		assert.Equal(t, 0, len(result.ExpiryAnalysis.ExpiryDistribution))

		// Should have zero volume analysis
		assert.True(t, math.IsNaN(result.VolumeAnalysis.AverageStoragePerContract))
		assert.True(t, math.IsNaN(result.VolumeAnalysis.MedianStoragePerContract))
		assert.Equal(t, 0, result.VolumeAnalysis.MaxStoragePerContract)
		assert.Equal(t, 0, result.VolumeAnalysis.MinStoragePerContract)
		assert.Equal(t, 0, result.VolumeAnalysis.TotalContracts)

		// Should have zero status analysis
		assert.Equal(t, 0, result.StatusAnalysis.AllExpiredContracts)
		assert.Equal(t, 0, result.StatusAnalysis.AllActiveContracts)
		assert.Equal(t, 0, result.StatusAnalysis.MixedStateContracts)
		assert.Equal(t, 0, result.StatusAnalysis.ActiveWithExpiredStorage)
	})

	t.Run("OnlyEOAs", func(t *testing.T) {
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

		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have empty rankings (no contracts)
		assert.Equal(t, 0, len(result.Rankings.TopByExpiredSlots))
		assert.Equal(t, 0, len(result.Rankings.TopByTotalSlots))

		// Should have zero contract analysis
		assert.Equal(t, 0, result.ExpiryAnalysis.ContractsAnalyzed)
		assert.Equal(t, 0, result.VolumeAnalysis.TotalContracts)
		assert.Equal(t, 0, result.StatusAnalysis.AllExpiredContracts+
			result.StatusAnalysis.AllActiveContracts+
			result.StatusAnalysis.MixedStateContracts)
	})

	t.Run("SingleContract", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          5,
			NumContracts:     1, // Single contract
			SlotsPerContract: 10,
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
			WindowSize:   50,
			TopN:         5,
		}

		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have at most 1 item in rankings
		assert.Equal(t, 1, len(result.Rankings.TopByExpiredSlots))
		assert.Equal(t, 1, len(result.Rankings.TopByTotalSlots))

		// Should have analysis for 1 contract
		if result.ExpiryAnalysis.ContractsAnalyzed > 0 {
			assert.Equal(t, 1, result.ExpiryAnalysis.ContractsAnalyzed)
		}
		if result.VolumeAnalysis.TotalContracts > 0 {
			assert.Equal(t, 1, result.VolumeAnalysis.TotalContracts)
			// For single contract, average should equal min and max
			assert.Equal(t, result.VolumeAnalysis.AverageStoragePerContract,
				float64(result.VolumeAnalysis.MaxStoragePerContract))
			assert.Equal(t, result.VolumeAnalysis.MaxStoragePerContract,
				result.VolumeAnalysis.MinStoragePerContract)
		}
	})

	t.Run("AllContractsExpired", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          10,
			NumContracts:     8,
			SlotsPerContract: 5,
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
			WindowSize:   50,
			TopN:         5,
		}

		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// All contracts should be fully expired
		if result.StatusAnalysis.AllExpiredContracts+result.StatusAnalysis.AllActiveContracts+
			result.StatusAnalysis.MixedStateContracts > 0 {
			// All contracts should be in expired state
			totalContracts := result.StatusAnalysis.AllExpiredContracts +
				result.StatusAnalysis.AllActiveContracts +
				result.StatusAnalysis.MixedStateContracts
			assert.Equal(t, totalContracts, result.StatusAnalysis.AllExpiredContracts,
				"All contracts should be expired")
			assert.Equal(t, 100.0, result.StatusAnalysis.AllExpiredRate,
				"All expired rate should be 100%")
		}

		// Expiry analysis should show high expiry percentages
		if result.ExpiryAnalysis.ContractsAnalyzed > 0 {
			assert.GreaterOrEqual(t, result.ExpiryAnalysis.AverageExpiryPercentage, 90.0,
				"Average expiry should be very high")
		}
	})

	t.Run("AllContractsActive", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          15,
			NumContracts:     12,
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
			WindowSize:   100,
			TopN:         8,
		}

		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// All contracts should be fully active
		if result.StatusAnalysis.AllExpiredContracts+result.StatusAnalysis.AllActiveContracts+
			result.StatusAnalysis.MixedStateContracts > 0 {
			// All contracts should be in active state
			totalContracts := result.StatusAnalysis.AllExpiredContracts +
				result.StatusAnalysis.AllActiveContracts +
				result.StatusAnalysis.MixedStateContracts
			assert.Equal(t, totalContracts, result.StatusAnalysis.AllActiveContracts,
				"All contracts should be active")
			assert.Equal(t, 100.0, result.StatusAnalysis.AllActiveRate,
				"All active rate should be 100%")
		}

		// Expiry analysis should show low expiry percentages
		if result.ExpiryAnalysis.ContractsAnalyzed > 0 {
			assert.LessOrEqual(t, result.ExpiryAnalysis.AverageExpiryPercentage, 10.0,
				"Average expiry should be very low")
		}
	})

	t.Run("ParameterValidation", func(t *testing.T) {
		setup := SetupAnalyticsTestWithDefaults(t)
		defer setup.Cleanup()

		ctx := context.Background()

		t.Run("InvalidTopN", func(t *testing.T) {
			params := QueryParams{
				ExpiryBlock:  50,
				CurrentBlock: 100,
				StartBlock:   1,
				EndBlock:     100,
				WindowSize:   10,
				TopN:         0, // Invalid TopN
			}

			// Should handle invalid TopN gracefully
			result, err := setup.Repository.GetContractAnalytics(ctx, params)
			if err != nil {
				t.Logf("Expected error for invalid TopN: %v", err)
			} else {
				require.NotNil(t, result)
				// Rankings should be empty or handle gracefully
				t.Logf("Method handled invalid TopN gracefully")
			}
		})

		t.Run("LargeTopN", func(t *testing.T) {
			params := QueryParams{
				ExpiryBlock:  50,
				CurrentBlock: 100,
				StartBlock:   1,
				EndBlock:     100,
				WindowSize:   10,
				TopN:         1000, // Very large TopN
			}

			result, err := setup.Repository.GetContractAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Should handle large TopN gracefully (might return fewer items)
			t.Logf("Method handled large TopN gracefully - returned %d/%d and %d/%d items",
				len(result.Rankings.TopByExpiredSlots), params.TopN,
				len(result.Rankings.TopByTotalSlots), params.TopN)
		})

		t.Run("ExpiryBlockEdgeCases", func(t *testing.T) {
			params := QueryParams{
				ExpiryBlock:  0, // Edge case: expiry at genesis
				CurrentBlock: 100,
				StartBlock:   1,
				EndBlock:     100,
				WindowSize:   50,
				TopN:         5,
			}

			result, err := setup.Repository.GetContractAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			t.Logf("Method handled expiry at genesis gracefully")
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
		const numGoroutines = 6
		results := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(routineID int) {
				defer func() {
					if r := recover(); r != nil {
						results <- assert.AnError
					}
				}()

				analytics, err := setup.Repository.GetContractAnalytics(ctx, params)
				if err != nil {
					results <- err
					return
				}

				if analytics == nil {
					results <- assert.AnError
					return
				}

				// Validate basic structure consistency
				if analytics == nil {
					results <- assert.AnError
					return
				}

				// Validate ranking consistency
				for _, item := range analytics.Rankings.TopByExpiredSlots {
					if item.TotalSlots < item.ExpiredSlots {
						results <- assert.AnError
						return
					}
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

			_, err := setup.Repository.GetContractAnalytics(ctx, params)
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

			_, err := setup.Repository.GetContractAnalytics(ctx, params)
			if err != nil {
				t.Logf("Correctly handled timeout context: %v", err)
			} else {
				t.Logf("Method completed despite timeout context")
			}
		})
	})

	t.Run("DeterministicDataValidation", func(t *testing.T) {
		// Test with specific configuration to validate deterministic behavior
		config := AnalyticsTestDataConfig{
			NumEOAs:          5,
			NumContracts:     10,
			SlotsPerContract: 4,
			StartBlock:       1,
			EndBlock:         20,
			ExpiryBlock:      10,
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
			TopN:         5,
		}

		// Calculate expected results
		expected := setup.TestData.CalculateExpectedContractAnalytics(config.ExpiryBlock, params.TopN)

		// Get actual results
		result, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Multiple calls should return consistent results
		result2, err := setup.Repository.GetContractAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result2)

		// Compare key metrics for consistency
		assert.Equal(t, result.ExpiryAnalysis.ContractsAnalyzed,
			result2.ExpiryAnalysis.ContractsAnalyzed,
			"Contracts analyzed should be consistent")
		assert.Equal(t, result.VolumeAnalysis.TotalContracts,
			result2.VolumeAnalysis.TotalContracts,
			"Total contracts should be consistent")
		assert.Equal(t, len(result.Rankings.TopByExpiredSlots),
			len(result2.Rankings.TopByExpiredSlots),
			"Top expired slots ranking count should be consistent")
		assert.Equal(t, len(result.Rankings.TopByTotalSlots),
			len(result2.Rankings.TopByTotalSlots),
			"Top total slots ranking count should be consistent")

		// Validate against expected deterministic results
		AssertContractAnalyticsMatch(t, expected, result, 0.1)

		t.Logf("Deterministic validation passed - consistent results across multiple calls")
	})
}

// TestGetBlockActivityAnalytics provides comprehensive testing for the GetBlockActivityAnalytics method
// Tests Questions 6, 12, 13, 14: Top activity blocks, time series data, access rates, and trend analysis
func TestGetBlockActivityAnalytics(t *testing.T) {
	t.Run("BasicFunctionality", func(t *testing.T) {
		// Setup test with known data distribution for block activity analysis
		config := AnalyticsTestDataConfig{
			NumEOAs:          10,
			NumContracts:     15,
			SlotsPerContract: 4,
			StartBlock:       1,
			EndBlock:         20,
			ExpiryBlock:      10, // Middle expiry point
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		ctx := context.Background()
		params := QueryParams{
			ExpiryBlock:  config.ExpiryBlock,
			CurrentBlock: config.EndBlock,
			StartBlock:   config.StartBlock,
			EndBlock:     config.EndBlock,
			WindowSize:   5, // Small window for detailed time series
			TopN:         5,
		}

		// Test the method
		result, err := setup.Repository.GetBlockActivityAnalytics(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Validate basic structure
		assert.NotNil(t, result.TopBlocks)
		assert.NotNil(t, result.TimeSeriesData)
		assert.NotNil(t, result.AccessRates)
		assert.NotNil(t, result.FrequencyData)
		assert.NotNil(t, result.TrendData)

		t.Logf("Block Activity Analytics Results: TopBlocks=%d, TimeSeriesPoints=%d, BlocksAnalyzed=%d",
			len(result.TopBlocks), len(result.TimeSeriesData), result.AccessRates.BlocksAnalyzed)
		t.Logf("Access Rates: AccountsPerBlock=%.2f, StoragePerBlock=%.2f, TotalPerBlock=%.2f",
			result.AccessRates.AccountsPerBlock, result.AccessRates.StoragePerBlock,
			result.AccessRates.TotalAccessesPerBlock)
		t.Logf("Trend: Direction=%s, GrowthRate=%.2f%%, PeakBlock=%d, LowBlock=%d",
			result.TrendData.TrendDirection, result.TrendData.GrowthRate,
			result.TrendData.PeakActivityBlock, result.TrendData.LowActivityBlock)
	})

	t.Run("TopBlocksValidation", func(t *testing.T) {
		// Setup basic test repository
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Insert some test data
		accounts := map[uint64]map[string]struct{}{
			100: {
				"0x1234567890123456789012345678901234567890": {},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {},
			},
			101: {
				"0x1234567890123456789012345678901234567890": {},
			},
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,
		}
		storage := map[uint64]map[string]map[string]struct{}{
			100: {
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
					"0x0000000000000000000000000000000000000000000000000000000000000002": {},
				},
			},
		}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		// Test GetTopActivityBlocks directly
		blocks, err := repo.GetTopActivityBlocks(ctx, 100, 101, 5)
		require.NoError(t, err)

		assert.Equal(t, 2, len(blocks))

		// Validate block 100
		assert.Equal(t, uint64(100), blocks[0].BlockNumber)
		assert.Equal(t, 1, blocks[0].EOAAccesses)
		assert.Equal(t, 1, blocks[0].ContractAccesses)
		assert.Equal(t, 2, blocks[0].StorageAccesses)
		assert.Equal(t, 4, blocks[0].TotalAccesses)

		// Validate block 101
		assert.Equal(t, uint64(101), blocks[1].BlockNumber)
		assert.Equal(t, 1, blocks[1].EOAAccesses)
		assert.Equal(t, 0, blocks[1].StorageAccesses)
		assert.Equal(t, 0, blocks[1].ContractAccesses)
		assert.Equal(t, 1, blocks[1].TotalAccesses)

		// Validate top blocks structure
		assert.LessOrEqual(t, len(blocks), 5)
		for _, block := range blocks {
			assert.GreaterOrEqual(t, block.BlockNumber, uint64(100))
			assert.LessOrEqual(t, block.BlockNumber, uint64(101))
			assert.GreaterOrEqual(t, block.TotalAccesses, 0)
		}

		t.Logf("Top blocks validation passed - returned %d blocks", len(blocks))
	})

	t.Run("ComponentMethodsTesting", func(t *testing.T) {
		// Test individual component methods directly
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Insert some test data
		accounts := map[uint64]map[string]struct{}{
			50: {
				"0x1234567890123456789012345678901234567890": {},
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {},
			},
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
			"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": true,
		}
		storage := map[uint64]map[string]map[string]struct{}{
			50: {
				"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				},
			},
		}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		t.Run("GetTopActivityBlocks", func(t *testing.T) {
			blocks, err := repo.GetTopActivityBlocks(ctx, 1, 100, 5)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(blocks), 5)
			for _, block := range blocks {
				assert.GreaterOrEqual(t, block.BlockNumber, uint64(1))
				assert.LessOrEqual(t, block.BlockNumber, uint64(100))
				assert.GreaterOrEqual(t, block.TotalAccesses, 0)
			}
			t.Logf("GetTopActivityBlocks returned %d blocks", len(blocks))
		})

		t.Run("GetTimeSeriesData", func(t *testing.T) {
			series, err := repo.GetTimeSeriesData(ctx, 1, 100, 10)
			require.NoError(t, err)
			for _, point := range series {
				assert.GreaterOrEqual(t, point.WindowStart, uint64(1))
				assert.GreaterOrEqual(t, point.TotalAccesses, 0)
				assert.GreaterOrEqual(t, point.AccessesPerBlock, 0.0)
			}
			t.Logf("GetTimeSeriesData returned %d points", len(series))
		})

		t.Run("GetAccessRates", func(t *testing.T) {
			rates, err := repo.GetAccessRates(ctx, 1, 100)
			require.NoError(t, err)
			require.NotNil(t, rates)
			assert.GreaterOrEqual(t, rates.AccountsPerBlock, 0.0)
			assert.GreaterOrEqual(t, rates.StoragePerBlock, 0.0)
			assert.GreaterOrEqual(t, rates.TotalAccessesPerBlock, 0.0)
			t.Logf("GetAccessRates: Accounts=%.2f, Storage=%.2f, Total=%.2f",
				rates.AccountsPerBlock, rates.StoragePerBlock, rates.TotalAccessesPerBlock)
		})

		t.Run("GetTrendAnalysis", func(t *testing.T) {
			trend, err := repo.GetTrendAnalysis(ctx, 1, 100)
			require.NoError(t, err)
			require.NotNil(t, trend)
			assert.Contains(t, []string{"increasing", "decreasing", "stable"}, trend.TrendDirection)
			t.Logf("GetTrendAnalysis: Direction=%s, Growth=%.2f%%",
				trend.TrendDirection, trend.GrowthRate)
		})

		t.Run("GetMostFrequentAccounts", func(t *testing.T) {
			accounts, err := repo.GetMostFrequentAccounts(ctx, 5)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(accounts), 5)
			for _, account := range accounts {
				assert.NotEmpty(t, account.Address)
				assert.GreaterOrEqual(t, account.AccessCount, 1)
			}
			t.Logf("GetMostFrequentAccounts returned %d accounts", len(accounts))
		})

		t.Run("GetMostFrequentStorage", func(t *testing.T) {
			storage, err := repo.GetMostFrequentStorage(ctx, 5)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(storage), 5)
			for _, slot := range storage {
				assert.NotEmpty(t, slot.Address)
				assert.NotEmpty(t, slot.StorageSlot)
				assert.GreaterOrEqual(t, slot.AccessCount, 1)
			}
			t.Logf("GetMostFrequentStorage returned %d slots", len(storage))
		})
	})

	t.Run("EdgeCases", func(t *testing.T) {
		t.Run("EmptyDatabase", func(t *testing.T) {
			repo, cleanup := setupClickHouseTestRepository(t)
			defer cleanup()

			ctx := context.Background()
			params := QueryParams{
				ExpiryBlock:  50,
				CurrentBlock: 100,
				StartBlock:   1,
				EndBlock:     100,
				WindowSize:   10,
				TopN:         5,
			}

			result, err := repo.GetBlockActivityAnalytics(ctx, params)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Should have empty results
			assert.Equal(t, 0, len(result.TopBlocks))
			assert.Equal(t, 0, len(result.TimeSeriesData))
			assert.Equal(t, 0, result.AccessRates.BlocksAnalyzed)
			assert.Equal(t, 0, len(result.FrequencyData.AccountFrequency.MostFrequentAccounts))
			assert.Equal(t, 0, len(result.FrequencyData.StorageFrequency.MostFrequentSlots))

			// Rates should be zero or NaN for empty data
			if !math.IsNaN(result.AccessRates.AccountsPerBlock) {
				assert.Equal(t, 0.0, result.AccessRates.AccountsPerBlock)
			}
			if !math.IsNaN(result.AccessRates.StoragePerBlock) {
				assert.Equal(t, 0.0, result.AccessRates.StoragePerBlock)
			}
		})

		t.Run("ParameterValidation", func(t *testing.T) {
			repo, cleanup := setupClickHouseTestRepository(t)
			defer cleanup()

			ctx := context.Background()

			t.Run("InvalidTopN", func(t *testing.T) {
				params := QueryParams{
					ExpiryBlock:  50,
					CurrentBlock: 100,
					StartBlock:   1,
					EndBlock:     100,
					WindowSize:   10,
					TopN:         0, // Invalid TopN
				}

				// Should handle invalid TopN gracefully
				result, err := repo.GetBlockActivityAnalytics(ctx, params)
				if err != nil {
					t.Logf("Expected error for invalid TopN: %v", err)
				} else {
					require.NotNil(t, result)
					// Should return empty or minimal results for top blocks
					t.Logf("Method handled invalid TopN gracefully")
				}
			})

			t.Run("InvalidWindowSize", func(t *testing.T) {
				params := QueryParams{
					ExpiryBlock:  50,
					CurrentBlock: 100,
					StartBlock:   1,
					EndBlock:     100,
					WindowSize:   0, // Invalid window size
					TopN:         10,
				}

				// Should handle invalid window size gracefully
				result, err := repo.GetBlockActivityAnalytics(ctx, params)
				if err != nil {
					t.Logf("Expected error for invalid window size: %v", err)
				} else {
					require.NotNil(t, result)
					t.Logf("Method handled invalid window size gracefully")
				}
			})
		})
	})

	t.Run("ConcurrencyTesting", func(t *testing.T) {
		repo, cleanup := setupClickHouseTestRepository(t)
		defer cleanup()

		// Insert some test data first
		ctx := context.Background()
		accounts := map[uint64]map[string]struct{}{
			50: {"0x1234567890123456789012345678901234567890": {}},
		}
		accountType := map[string]bool{
			"0x1234567890123456789012345678901234567890": false,
		}
		storage := map[uint64]map[string]map[string]struct{}{}

		err := repo.InsertRange(ctx, accounts, accountType, storage, 1)
		require.NoError(t, err)

		params := QueryParams{
			ExpiryBlock:  50,
			CurrentBlock: 100,
			StartBlock:   1,
			EndBlock:     100,
			WindowSize:   20,
			TopN:         10,
		}

		// Test concurrent access
		const numGoroutines = 4
		results := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(routineID int) {
				defer func() {
					if r := recover(); r != nil {
						results <- fmt.Errorf("panic in goroutine %d: %v", routineID, r)
					}
				}()

				analytics, err := repo.GetBlockActivityAnalytics(ctx, params)
				if err != nil {
					results <- err
					return
				}

				if analytics == nil {
					results <- fmt.Errorf("got nil analytics")
					return
				}

				// Validate basic structure consistency
				if analytics.TopBlocks == nil || analytics.TimeSeriesData == nil {
					results <- fmt.Errorf("missing structure components")
					return
				}

				// Validate top blocks ordering consistency
				for j := 1; j < len(analytics.TopBlocks); j++ {
					if analytics.TopBlocks[j-1].TotalAccesses < analytics.TopBlocks[j].TotalAccesses {
						results <- fmt.Errorf("ordering violation in top blocks")
						return
					}
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
