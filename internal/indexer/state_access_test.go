package indexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

// TestFixtures contains realistic test data for state access testing
type TestFixtures struct {
	// Account addresses
	EOAAddress1      string
	EOAAddress2      string
	ContractAddress1 string
	ContractAddress2 string

	// Storage slots
	StorageSlot1 string
	StorageSlot2 string
	StorageSlot3 string

	// Block numbers
	Block100 uint64
	Block200 uint64
	Block300 uint64
}

// createTestFixtures generates realistic test data for state access testing
func createTestFixtures() TestFixtures {
	return TestFixtures{
		// Realistic Ethereum addresses (40 characters)
		EOAAddress1:      "0x1111111111111111111111111111111111111111",
		EOAAddress2:      "0x2222222222222222222222222222222222222222",
		ContractAddress1: "0x3333333333333333333333333333333333333333",
		ContractAddress2: "0x4444444444444444444444444444444444444444",

		// Realistic storage slots (64 characters)
		StorageSlot1: "0x0000000000000000000000000000000000000000000000000000000000000001",
		StorageSlot2: "0x0000000000000000000000000000000000000000000000000000000000000002",
		StorageSlot3: "0x0000000000000000000000000000000000000000000000000000000000000003",

		// Block numbers
		Block100: 100,
		Block200: 200,
		Block300: 300,
	}
}

// createTransactionResult creates a realistic TransactionResult for testing
func createTransactionResult(txHash string, stateDiff map[string]rpc.AccountDiff) *rpc.TransactionResult {
	return &rpc.TransactionResult{
		TxHash:    txHash,
		StateDiff: stateDiff,
	}
}

// createAccountDiff creates an AccountDiff with the specified components
func createAccountDiff(hasBalance, hasCode, hasNonce, hasStorage bool, storageSlots []string) rpc.AccountDiff {
	diff := rpc.AccountDiff{}

	if hasBalance {
		diff.Balance = map[string]any{
			"from": "0x0",
			"to":   "0x1000000000000000000",
		}
	}

	if hasCode {
		diff.Code = map[string]any{
			"from": "0x",
			"to":   "0x608060405234801561001057600080fd5b50",
		}
	}

	if hasNonce {
		diff.Nonce = map[string]any{
			"from": "0x0",
			"to":   "0x1",
		}
	}

	if hasStorage && len(storageSlots) > 0 {
		storage := make(map[string]any)
		for _, slot := range storageSlots {
			storage[slot] = map[string]any{
				"from": "0x0000000000000000000000000000000000000000000000000000000000000000",
				"to":   "0x0000000000000000000000000000000000000000000000000000000000000001",
			}
		}
		diff.Storage = storage
	}

	return diff
}

// TestStateAccessLatest tests the latest-mode state access implementation
func TestStateAccessLatest(t *testing.T) {
	fixtures := createTestFixtures()

	t.Run("AddAccount basic functionality", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add EOA account
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count())

		// Add contract account
		err = sa.AddAccount(fixtures.ContractAddress1, fixtures.Block200, true)
		assert.NoError(t, err)
		assert.Equal(t, 2, sa.Count())

		// Verify accounts map
		assert.Equal(t, fixtures.Block100, sa.accounts[fixtures.EOAAddress1])
		assert.Equal(t, fixtures.Block200, sa.accounts[fixtures.ContractAddress1])

		// Verify account types
		assert.False(t, sa.accountType[fixtures.EOAAddress1])
		assert.True(t, sa.accountType[fixtures.ContractAddress1])
	})

	t.Run("AddAccount deduplication", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add same account multiple times
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count())

		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count()) // Count should not increase

		// Latest block should be stored
		assert.Equal(t, fixtures.Block200, sa.accounts[fixtures.EOAAddress1])
	})

	t.Run("AddAccount type upgrade from EOA to Contract", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add as EOA first
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.False(t, sa.accountType[fixtures.EOAAddress1])

		// Upgrade to contract
		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block200, true)
		assert.NoError(t, err)
		assert.True(t, sa.accountType[fixtures.EOAAddress1]) // Should be upgraded to contract

		// Try to downgrade back to EOA (should remain contract)
		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block300, false)
		assert.NoError(t, err)
		assert.True(t, sa.accountType[fixtures.EOAAddress1]) // Should remain contract
	})

	t.Run("AddStorage basic functionality", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add storage slots
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		assert.Equal(t, 1, sa.Count())

		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot2, fixtures.Block200)
		assert.Equal(t, 2, sa.Count())

		// Add storage for different contract
		sa.AddStorage(fixtures.ContractAddress2, fixtures.StorageSlot1, fixtures.Block300)
		assert.Equal(t, 3, sa.Count())

		// Verify storage structure
		assert.Equal(t, fixtures.Block100, sa.storage[fixtures.ContractAddress1][fixtures.StorageSlot1])
		assert.Equal(t, fixtures.Block200, sa.storage[fixtures.ContractAddress1][fixtures.StorageSlot2])
		assert.Equal(t, fixtures.Block300, sa.storage[fixtures.ContractAddress2][fixtures.StorageSlot1])
	})

	t.Run("AddStorage deduplication", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add same storage slot multiple times
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		assert.Equal(t, 1, sa.Count())

		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)
		assert.Equal(t, 1, sa.Count()) // Count should not increase

		// Latest block should be stored
		assert.Equal(t, fixtures.Block200, sa.storage[fixtures.ContractAddress1][fixtures.StorageSlot1])
	})

	t.Run("Reset functionality", func(t *testing.T) {
		sa := newStateAccessLatest()

		// Add some data
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		assert.Equal(t, 2, sa.Count())

		// Reset
		sa.Reset()
		assert.Equal(t, 0, sa.Count())
		assert.Empty(t, sa.accounts)
		assert.Empty(t, sa.accountType)
		assert.Empty(t, sa.storage)
	})
}

// TestStateAccessArchive tests the archive-mode state access implementation
func TestStateAccessArchive(t *testing.T) {
	fixtures := createTestFixtures()

	t.Run("AddAccount basic functionality", func(t *testing.T) {
		sa := newStateAccessArchive()

		// Add accounts to different blocks
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count())

		err = sa.AddAccount(fixtures.ContractAddress1, fixtures.Block200, true)
		assert.NoError(t, err)
		assert.Equal(t, 2, sa.Count())

		// Verify accountsByBlock structure
		assert.Contains(t, sa.accountsByBlock[fixtures.Block100], fixtures.EOAAddress1)
		assert.Contains(t, sa.accountsByBlock[fixtures.Block200], fixtures.ContractAddress1)

		// Verify account types
		assert.False(t, sa.accountType[fixtures.EOAAddress1])
		assert.True(t, sa.accountType[fixtures.ContractAddress1])
	})

	t.Run("AddAccount archive mode stores all events", func(t *testing.T) {
		sa := newStateAccessArchive()

		// Add same account to multiple blocks
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count())

		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		assert.NoError(t, err)
		assert.Equal(t, 2, sa.Count()) // Count should increase

		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block300, false)
		assert.NoError(t, err)
		assert.Equal(t, 3, sa.Count()) // Count should increase again

		// All events should be stored
		assert.Contains(t, sa.accountsByBlock[fixtures.Block100], fixtures.EOAAddress1)
		assert.Contains(t, sa.accountsByBlock[fixtures.Block200], fixtures.EOAAddress1)
		assert.Contains(t, sa.accountsByBlock[fixtures.Block300], fixtures.EOAAddress1)
	})

	t.Run("AddAccount same block deduplication", func(t *testing.T) {
		sa := newStateAccessArchive()

		// Add same account multiple times to same block
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count())

		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, sa.Count()) // Count should not increase for same block

		// Only one entry should exist for this block
		assert.Len(t, sa.accountsByBlock[fixtures.Block100], 1)
		assert.Contains(t, sa.accountsByBlock[fixtures.Block100], fixtures.EOAAddress1)
	})

	t.Run("AddStorage archive mode stores all events", func(t *testing.T) {
		sa := newStateAccessArchive()

		// Add storage slots to different blocks
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		assert.Equal(t, 1, sa.Count())

		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)
		assert.Equal(t, 2, sa.Count()) // Count should increase

		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot2, fixtures.Block100)
		assert.Equal(t, 3, sa.Count()) // Count should increase

		// All events should be stored
		assert.Contains(t, sa.storageByBlock[fixtures.Block100][fixtures.ContractAddress1], fixtures.StorageSlot1)
		assert.Contains(t, sa.storageByBlock[fixtures.Block200][fixtures.ContractAddress1], fixtures.StorageSlot1)
		assert.Contains(t, sa.storageByBlock[fixtures.Block100][fixtures.ContractAddress1], fixtures.StorageSlot2)
	})

	t.Run("Reset functionality", func(t *testing.T) {
		sa := newStateAccessArchive()

		// Add some data
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		assert.NoError(t, err)
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		assert.Equal(t, 2, sa.Count())

		// Reset
		sa.Reset()
		assert.Equal(t, 0, sa.Count())
		assert.Empty(t, sa.accountsByBlock)
		assert.Empty(t, sa.accountType)
		assert.Empty(t, sa.storageByBlock)
	})
}

// TestStateAccessCommit tests the commit functionality with database integration
func TestStateAccessCommit(t *testing.T) {
	fixtures := createTestFixtures()

	t.Run("StateAccessLatest commit to PostgreSQL", func(t *testing.T) {
		// Setup PostgreSQL test database
		repo, cleanup := setupTestRepository(t, false)
		defer cleanup()

		sa := newStateAccessLatest()
		ctx := context.Background()

		// Add test data
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = sa.AddAccount(fixtures.ContractAddress1, fixtures.Block200, true)
		require.NoError(t, err)
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)

		// Commit to database
		err = sa.Commit(ctx, repo, 1)
		assert.NoError(t, err)

		// Verify data was persisted
		// Note: This would require additional methods to verify data in the repository
		// For now, we just verify the commit didn't fail
	})

	t.Run("StateAccessArchive commit to ClickHouse", func(t *testing.T) {
		// Setup ClickHouse test database
		repo, cleanup := setupTestRepository(t, true)
		defer cleanup()

		sa := newStateAccessArchive()
		ctx := context.Background()

		// Add test data with multiple blocks
		err := sa.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = sa.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		require.NoError(t, err)
		err = sa.AddAccount(fixtures.ContractAddress1, fixtures.Block200, true)
		require.NoError(t, err)
		sa.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)

		// Commit to database
		err = sa.Commit(ctx, repo, 1)
		assert.NoError(t, err)
	})

	t.Run("StateAccessLatest cannot use archive method", func(t *testing.T) {
		// Setup PostgreSQL test database
		repo, cleanup := setupTestRepository(t, false)
		defer cleanup()

		ctx := context.Background()

		// Try to use archive method (should fail)
		err := repo.UpdateRangeDataWithAllEventsInTx(ctx,
			map[uint64]map[string]struct{}{},
			map[string]bool{},
			map[uint64]map[string]map[string]struct{}{},
			1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "archive mode")
	})
}

// TestStateAccessBehaviorDifferences tests the key differences between latest and archive modes
func TestStateAccessBehaviorDifferences(t *testing.T) {
	fixtures := createTestFixtures()

	t.Run("Deduplication behavior differences", func(t *testing.T) {
		// Test latest mode (deduplication)
		saLatest := newStateAccessLatest()

		// Add same account multiple times
		err := saLatest.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = saLatest.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		require.NoError(t, err)
		err = saLatest.AddAccount(fixtures.EOAAddress1, fixtures.Block300, false)
		require.NoError(t, err)

		// Latest mode should only count unique accounts
		assert.Equal(t, 1, saLatest.Count())

		// Test archive mode (no deduplication across blocks)
		saArchive := newStateAccessArchive()

		// Add same account multiple times to different blocks
		err = saArchive.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = saArchive.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		require.NoError(t, err)
		err = saArchive.AddAccount(fixtures.EOAAddress1, fixtures.Block300, false)
		require.NoError(t, err)

		// Archive mode should count all access events
		assert.Equal(t, 3, saArchive.Count())
	})

	t.Run("Storage deduplication behavior differences", func(t *testing.T) {
		// Test latest mode
		saLatest := newStateAccessLatest()

		// Add same storage slot multiple times
		saLatest.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		saLatest.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)
		saLatest.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block300)

		// Latest mode should only count unique slots
		assert.Equal(t, 1, saLatest.Count())

		// Test archive mode
		saArchive := newStateAccessArchive()

		// Add same storage slot to different blocks
		saArchive.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block100)
		saArchive.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block200)
		saArchive.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, fixtures.Block300)

		// Archive mode should count all access events
		assert.Equal(t, 3, saArchive.Count())
	})

	t.Run("Data structure differences", func(t *testing.T) {
		fixtures := createTestFixtures()

		// Latest mode stores: address -> latest_block
		saLatest := newStateAccessLatest()
		err := saLatest.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = saLatest.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		require.NoError(t, err)

		// Should only store latest block
		assert.Equal(t, fixtures.Block200, saLatest.accounts[fixtures.EOAAddress1])

		// Archive mode stores: block -> set of addresses
		saArchive := newStateAccessArchive()
		err = saArchive.AddAccount(fixtures.EOAAddress1, fixtures.Block100, false)
		require.NoError(t, err)
		err = saArchive.AddAccount(fixtures.EOAAddress1, fixtures.Block200, false)
		require.NoError(t, err)

		// Should store all block access events
		assert.Contains(t, saArchive.accountsByBlock[fixtures.Block100], fixtures.EOAAddress1)
		assert.Contains(t, saArchive.accountsByBlock[fixtures.Block200], fixtures.EOAAddress1)
	})
}

// TestStateAccessMemoryManagement tests memory usage patterns
func TestStateAccessMemoryManagement(t *testing.T) {
	t.Run("Large dataset handling", func(t *testing.T) {
		saLatest := newStateAccessLatest()
		saArchive := newStateAccessArchive()

		// Add a large number of unique accounts
		accountCount := 10000
		for i := 0; i < accountCount; i++ {
			addr := generateTestAddress(i)
			block := uint64(1000 + i)

			err := saLatest.AddAccount(addr, block, i%2 == 0)
			require.NoError(t, err)

			err = saArchive.AddAccount(addr, block, i%2 == 0)
			require.NoError(t, err)
		}

		// Verify counts
		assert.Equal(t, accountCount, saLatest.Count())
		assert.Equal(t, accountCount, saArchive.Count())

		// Add storage slots for contracts (every other account)
		storageCount := 0
		for i := 0; i < accountCount; i++ {
			if i%2 == 0 { // Contracts only
				addr := generateTestAddress(i)
				for j := 0; j < 5; j++ { // 5 storage slots per contract
					slot := generateTestSlot(j)
					block := uint64(1000 + i + j)

					saLatest.AddStorage(addr, slot, block)
					saArchive.AddStorage(addr, slot, block)
					storageCount++
				}
			}
		}

		// Verify final counts
		expectedTotal := accountCount + storageCount
		assert.Equal(t, expectedTotal, saLatest.Count())
		assert.Equal(t, expectedTotal, saArchive.Count())

		// Test reset frees memory
		saLatest.Reset()
		saArchive.Reset()
		assert.Equal(t, 0, saLatest.Count())
		assert.Equal(t, 0, saArchive.Count())
	})
}

// TestStateAccessEdgeCases tests edge cases and error conditions
func TestStateAccessEdgeCases(t *testing.T) {
	fixtures := createTestFixtures()

	t.Run("Empty data handling", func(t *testing.T) {
		saLatest := newStateAccessLatest()
		saArchive := newStateAccessArchive()

		// Test with empty data
		assert.Equal(t, 0, saLatest.Count())
		assert.Equal(t, 0, saArchive.Count())

		// Reset empty state access
		saLatest.Reset()
		saArchive.Reset()
		assert.Equal(t, 0, saLatest.Count())
		assert.Equal(t, 0, saArchive.Count())
	})

	t.Run("Zero block number handling", func(t *testing.T) {
		saLatest := newStateAccessLatest()
		saArchive := newStateAccessArchive()

		// Add accounts at block 0 (genesis)
		err := saLatest.AddAccount(fixtures.EOAAddress1, 0, false)
		assert.NoError(t, err)
		err = saArchive.AddAccount(fixtures.EOAAddress1, 0, false)
		assert.NoError(t, err)

		// Add storage at block 0
		saLatest.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, 0)
		saArchive.AddStorage(fixtures.ContractAddress1, fixtures.StorageSlot1, 0)

		assert.Equal(t, 2, saLatest.Count())
		assert.Equal(t, 2, saArchive.Count())
	})

	t.Run("Empty address handling", func(t *testing.T) {
		saLatest := newStateAccessLatest()
		saArchive := newStateAccessArchive()

		// Add empty address (should work but not be realistic)
		err := saLatest.AddAccount("", fixtures.Block100, false)
		assert.NoError(t, err)
		err = saArchive.AddAccount("", fixtures.Block100, false)
		assert.NoError(t, err)

		// Add storage with empty address
		saLatest.AddStorage("", fixtures.StorageSlot1, fixtures.Block100)
		saArchive.AddStorage("", fixtures.StorageSlot1, fixtures.Block100)

		assert.Equal(t, 2, saLatest.Count())
		assert.Equal(t, 2, saArchive.Count())
	})

	t.Run("Very large block numbers", func(t *testing.T) {
		saLatest := newStateAccessLatest()
		saArchive := newStateAccessArchive()

		largeBlock := uint64(18446744073709551615) // Max uint64

		err := saLatest.AddAccount(fixtures.EOAAddress1, largeBlock, false)
		assert.NoError(t, err)
		err = saArchive.AddAccount(fixtures.EOAAddress1, largeBlock, false)
		assert.NoError(t, err)

		assert.Equal(t, 1, saLatest.Count())
		assert.Equal(t, 1, saArchive.Count())
	})
}

// Helper functions for testing

// generateTestAddress creates a test Ethereum address
func generateTestAddress(index int) string {
	return "0x" + padHex(index, 40)
}

// generateTestSlot creates a test storage slot key
func generateTestSlot(index int) string {
	return "0x" + padHex(index, 64)
}

// padHex pads an integer to a hex string of specified length
func padHex(value, length int) string {
	hex := ""
	for i := 0; i < length; i++ {
		if i < 8 { // Use the value for the last 8 hex digits
			digit := (value >> ((7 - i) * 4)) & 0xF
			if digit < 10 {
				hex += string(rune('0' + digit))
			} else {
				hex += string(rune('a' + digit - 10))
			}
		} else {
			hex += "0" // Pad with zeros
		}
	}
	return hex
}

// setupTestRepository creates a test repository with clean database
func setupTestRepository(t *testing.T, archiveMode bool) (repository.StateRepositoryInterface, func()) {
	t.Helper()

	cleanup := testdb.SetupTestDatabase(t, archiveMode)

	// Get test configuration
	testConfig := testdb.GetTestConfig()

	var config internal.Config
	if archiveMode {
		config = internal.Config{
			ClickHouseHost:     testConfig.ClickHouse.Host,
			ClickHousePort:     testConfig.ClickHouse.Port,
			ClickHouseUser:     testConfig.ClickHouse.User,
			ClickHousePassword: testConfig.ClickHouse.Password,
			ClickHouseDatabase: testConfig.ClickHouse.Database,
			ClickHouseMaxConns: 10,
			ClickHouseMinConns: 2,
			RPCURLS:            []string{"http://localhost:8545"},
			Environment:        "test",
			ArchiveMode:        true,
		}
	} else {
		config = internal.Config{
			DBHost:      testConfig.PostgreSQL.Host,
			DBPort:      testConfig.PostgreSQL.Port,
			DBUser:      testConfig.PostgreSQL.User,
			DBPassword:  testConfig.PostgreSQL.Password,
			DBName:      testConfig.PostgreSQL.Database,
			DBMaxConns:  10,
			DBMinConns:  2,
			RPCURLS:     []string{"http://localhost:8545"},
			Environment: "test",
			ArchiveMode: false,
		}
	}

	ctx := context.Background()
	repo, err := repository.NewRepository(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	return repo, cleanup
}
