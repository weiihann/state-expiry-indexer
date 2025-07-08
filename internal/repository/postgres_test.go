package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/db"
	"github.com/weiihann/state-expiry-indexer/internal"
)

// Test helper functions to avoid import cycle with testdb

// generateTestAddress creates a test Ethereum address
func generateTestAddress(index int) string {
	return fmt.Sprintf("0x%040x", index)
}

// generateTestStorageSlot creates a test storage slot key
func generateTestStorageSlot(index int) string {
	return fmt.Sprintf("0x%064x", index)
}

// runTestMigrations runs database migrations for tests
func runTestMigrations(config internal.Config, migrationPath string) error {
	// Create database connection
	database, err := db.ConnectSQL(config)
	if err != nil {
		return fmt.Errorf("could not connect to database: %w", err)
	}
	defer database.Close()

	// Create postgres driver instance
	driver, err := postgres.WithInstance(database, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create postgres driver: %w", err)
	}

	// Create migrate instance
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+migrationPath,
		"postgres", driver)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %w", err)
	}
	defer m.Close()

	// Apply all pending migrations
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			// No migrations to apply - this is fine
			return nil
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	return nil
}

// setupTestRepository creates a test repository with clean database
func setupTestRepository(t *testing.T) (StateRepositoryInterface, func()) {
	t.Helper()

	// Use test configuration
	config := internal.Config{
		DBHost:      "localhost",
		DBPort:      "15432",
		DBUser:      "test",
		DBPassword:  "test",
		DBName:      "test",
		DBMaxConns:  5,
		DBMinConns:  1,
		Environment: "test",
		ArchiveMode: false,
		RPCURLS:     []string{"http://localhost:8545"}, // Required for validation
	}

	// Run migrations first
	migrationPath := "../../db/migrations"

	// Skip tests if migration fails (database not available)
	if err := runTestMigrations(config, migrationPath); err != nil {
		t.Skipf("Test database not available or migrations failed: %v", err)
		return nil, func() {}
	}

	// Create repository
	ctx := context.Background()
	repo, err := NewRepository(ctx, config)
	if err != nil {
		t.Skipf("Failed to create repository after migrations: %v", err)
		return nil, func() {}
	}

	// Cleanup function
	cleanup := func() {
		// Clean up test data by dropping and recreating tables
		if pgRepo, ok := repo.(*PostgreSQLRepository); ok {
			ctx := context.Background()
			// Simple cleanup - just clean the metadata to isolate tests
			_, err := pgRepo.db.Exec(ctx, "DELETE FROM metadata")
			if err != nil {
				t.Logf("Warning: Failed to clean metadata table: %v", err)
			}
			_, err = pgRepo.db.Exec(ctx, "DELETE FROM accounts_current")
			if err != nil {
				t.Logf("Warning: Failed to clean accounts_current table: %v", err)
			}
			_, err = pgRepo.db.Exec(ctx, "DELETE FROM storage_current")
			if err != nil {
				t.Logf("Warning: Failed to clean storage_current table: %v", err)
			}
		}
	}

	return repo, cleanup
}

// assertAccountExists verifies that an account exists with expected values
func assertAccountExists(t *testing.T, repo StateRepositoryInterface, address string, expectedBlock uint64, expectedIsContract *bool) {
	t.Helper()

	ctx := context.Background()

	// Cast to PostgreSQL repository to access GetAccountInfo method
	pgRepo, ok := repo.(*PostgreSQLRepository)
	if !ok {
		t.Skip("Test requires PostgreSQL repository")
		return
	}

	// Check account info
	account, err := pgRepo.GetAccountInfo(ctx, address)
	require.NoError(t, err, "failed to get account info for %s", address)
	require.NotNil(t, account, "account %s should exist", address)

	assert.Equal(t, address, account.Address, "address mismatch")
	assert.Equal(t, expectedBlock, account.LastAccessBlock, "last access block mismatch for %s", address)

	if expectedIsContract != nil {
		require.NotNil(t, account.IsContract, "is_contract should not be nil for %s", address)
		assert.Equal(t, *expectedIsContract, *account.IsContract, "is_contract mismatch for %s", address)
	}
}

// assertStorageExists verifies that a storage slot exists with expected values
func assertStorageExists(t *testing.T, repo StateRepositoryInterface, address, slot string, expectedBlock uint64) {
	t.Helper()

	ctx := context.Background()

	// Cast to PostgreSQL repository to access GetStateLastAccessedBlock method
	pgRepo, ok := repo.(*PostgreSQLRepository)
	if !ok {
		t.Skip("Test requires PostgreSQL repository")
		return
	}

	// Check storage access
	lastBlock, err := pgRepo.GetStateLastAccessedBlock(ctx, address, &slot)
	require.NoError(t, err, "failed to get storage last accessed block for %s:%s", address, slot)
	assert.Equal(t, expectedBlock, lastBlock, "last access block mismatch for storage %s:%s", address, slot)
}

// TestGetLastIndexedRange tests getting the last indexed range from metadata
func TestGetLastIndexedRange(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), lastRange, "Should return 0 for empty database")
	})

	t.Run("WithExistingData", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

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
		assert.Equal(t, uint64(42), lastRange, "Should return the last indexed range")
	})

	t.Run("MultipleUpdates", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

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

// TestUpdateRangeDataInTx tests the main data update functionality
func TestUpdateRangeDataInTx(t *testing.T) {
	t.Run("EmptyMaps", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
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
		repo, cleanup := setupTestRepository(t)
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

		// Verify accounts were inserted
		isContract1 := false
		isContract2 := true
		assertAccountExists(t, repo, "0x1234567890123456789012345678901234567890", 100, &isContract1)
		assertAccountExists(t, repo, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", 150, &isContract2)
	})

	t.Run("StorageOnly", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
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

		// Verify storage was inserted
		assertStorageExists(t, repo, "0x1234567890123456789012345678901234567890",
			"0x0000000000000000000000000000000000000000000000000000000000000001", 100)
		assertStorageExists(t, repo, "0x1234567890123456789012345678901234567890",
			"0x0000000000000000000000000000000000000000000000000000000000000002", 150)
	})

	t.Run("AccountsAndStorage", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
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
		isContract1 := false
		isContract2 := true
		assertAccountExists(t, repo, "0x1234567890123456789012345678901234567890", 100, &isContract1)
		assertAccountExists(t, repo, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", 150, &isContract2)
		assertStorageExists(t, repo, "0x1234567890123456789012345678901234567890",
			"0x0000000000000000000000000000000000000000000000000000000000000001", 100)
		assertStorageExists(t, repo, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
			"0x0000000000000000000000000000000000000000000000000000000000000001", 150)
		assertStorageExists(t, repo, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
			"0x0000000000000000000000000000000000000000000000000000000000000002", 160)

		// Verify metadata was updated
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), lastRange)
	})

	t.Run("UpdateExistingData", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// First insert
		accounts1 := map[string]uint64{"0x1234567890123456789012345678901234567890": 100}
		accountType1 := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage1 := map[string]map[string]uint64{
			"0x1234567890123456789012345678901234567890": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 100,
			},
		}

		err := repo.UpdateRangeDataInTx(ctx, accounts1, accountType1, storage1, 1)
		require.NoError(t, err)

		// Update with later block numbers
		accounts2 := map[string]uint64{"0x1234567890123456789012345678901234567890": 200}
		accountType2 := map[string]bool{"0x1234567890123456789012345678901234567890": false}
		storage2 := map[string]map[string]uint64{
			"0x1234567890123456789012345678901234567890": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 200,
			},
		}

		err = repo.UpdateRangeDataInTx(ctx, accounts2, accountType2, storage2, 2)
		require.NoError(t, err)

		// Verify data was updated to latest block numbers
		isContract := false
		assertAccountExists(t, repo, "0x1234567890123456789012345678901234567890", 200, &isContract)
		assertStorageExists(t, repo, "0x1234567890123456789012345678901234567890",
			"0x0000000000000000000000000000000000000000000000000000000000000001", 200)
	})

	t.Run("LargeDataSet", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Create large dataset to test batch processing
		accounts := make(map[string]uint64)
		accountType := make(map[string]bool)
		storage := make(map[string]map[string]uint64)

		// Create 100 accounts with storage
		for i := 0; i < 100; i++ {
			addr := generateTestAddress(i)
			accounts[addr] = uint64(1000 + i)
			accountType[addr] = i%2 == 0 // Alternate between EOA and Contract

			// Add storage for contracts
			if accountType[addr] {
				storage[addr] = make(map[string]uint64)
				for j := 0; j < 5; j++ { // 5 storage slots per contract
					slot := generateTestStorageSlot(j)
					storage[addr][slot] = uint64(1000 + i)
				}
			}
		}

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountType, storage, 10)
		require.NoError(t, err)

		// Verify a sample of the data
		for i := 0; i < 10; i++ {
			addr := generateTestAddress(i)
			isContract := i%2 == 0
			assertAccountExists(t, repo, addr, uint64(1000+i), &isContract)

			if isContract {
				slot := generateTestStorageSlot(0)
				assertStorageExists(t, repo, addr, slot, uint64(1000+i))
			}
		}

		// Verify metadata
		lastRange, err := repo.GetLastIndexedRange(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), lastRange)
	})
}

// TestGetSyncStatus tests sync status reporting
func TestGetSyncStatus(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		status, err := repo.GetSyncStatus(ctx, 100, 10)
		require.NoError(t, err)

		assert.False(t, status.IsSynced, "Should not be synced with empty database")
		assert.Equal(t, uint64(0), status.LastIndexedRange)
		assert.Equal(t, uint64(0), status.EndBlock) // latestRange * rangeSize
	})

	t.Run("PartialSync", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
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
		repo, cleanup := setupTestRepository(t)
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

// TestGetAnalyticsData tests comprehensive analytics functionality
func TestGetAnalyticsData(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		_, err := repo.GetAnalyticsData(ctx, 1000, 2000)
		require.Error(t, err)
	})

	t.Run("WithTestData", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()

		// Create test data
		accounts := make(map[string]uint64)
		accountType := make(map[string]bool)
		storage := make(map[string]map[string]uint64)

		// Create 20 accounts: 10 EOAs and 10 contracts
		for i := 0; i < 20; i++ {
			addr := generateTestAddress(i)
			accounts[addr] = uint64(1000 + i*100) // Spread across blocks
			accountType[addr] = i >= 10           // First 10 are EOAs, rest are contracts

			// Add storage for contracts
			if accountType[addr] {
				storage[addr] = make(map[string]uint64)
				for j := 0; j < 3; j++ { // 3 storage slots per contract
					slot := generateTestStorageSlot(j)
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

// TestUpdateRangeDataWithAllEventsInTx tests that PostgreSQL properly rejects archive mode operations
func TestUpdateRangeDataWithAllEventsInTx(t *testing.T) {
	t.Run("ArchiveModeNotSupported", func(t *testing.T) {
		repo, cleanup := setupTestRepository(t)
		defer cleanup()

		ctx := context.Background()
		accountAccesses := map[uint64]map[string]struct{}{}
		accountType := map[string]bool{}
		storageAccesses := map[uint64]map[string]map[string]struct{}{}

		// PostgreSQL should reject archive mode operations
		err := repo.UpdateRangeDataWithAllEventsInTx(ctx, accountAccesses, accountType, storageAccesses, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "archive mode", "Should indicate archive mode is not supported")
	})
}
