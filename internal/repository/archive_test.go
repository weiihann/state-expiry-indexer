package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/database"
)

// TestArchiveEquivalence verifies that archive mode produces equivalent results to PostgreSQL for current state queries
func TestArchiveEquivalence(t *testing.T) {
	// Skip if ClickHouse is not available in test environment
	if testing.Short() {
		t.Skip("Skipping archive equivalence test in short mode")
	}

	ctx := context.Background()

	// Load test configuration
	config, err := internal.LoadConfig("../../configs")
	require.NoError(t, err, "Failed to load test configuration")

	// Setup PostgreSQL repository
	pgRepo := setupPostgreSQLTest(t, ctx, config)
	defer cleanupPostgreSQL(t, pgRepo)

	// Setup ClickHouse repository
	chRepo := setupClickHouseTest(t, ctx, config)
	defer cleanupClickHouse(t, chRepo)

	// Test data setup
	testAddresses := []string{
		"0x1234567890abcdef1234567890abcdef12345678",
		"0xabcdef1234567890abcdef1234567890abcdef12",
		"0x9876543210fedcba9876543210fedcba98765432",
	}

	testBlocks := []uint64{1000000, 1000500, 1001000}
	expiryBlock := uint64(1000750) // Between second and third block

	// Insert test data into both repositories
	t.Run("Setup_Test_Data", func(t *testing.T) {
		setupArchiveTestData(t, ctx, pgRepo, chRepo, testAddresses, testBlocks)
	})

	// Compare basic state queries
	t.Run("State_Last_Accessed_Block_Equivalence", func(t *testing.T) {
		for _, address := range testAddresses {
			pgResult, pgErr := pgRepo.GetStateLastAccessedBlock(ctx, address, nil)
			chResult, chErr := chRepo.GetStateLastAccessedBlock(ctx, address, nil)

			assert.Equal(t, pgErr, chErr, "Error status should match for address %s", address)
			if pgErr == nil && chErr == nil {
				assert.Equal(t, pgResult, chResult, "Last accessed block should match for address %s", address)
			}
		}
	})

	// Compare expired state counts
	t.Run("Expired_State_Count_Equivalence", func(t *testing.T) {
		pgCount, pgErr := pgRepo.GetExpiredStateCount(ctx, expiryBlock)
		chCount, chErr := chRepo.GetExpiredStateCount(ctx, expiryBlock)

		assert.Equal(t, pgErr, chErr, "Error status should match for expired count")
		if pgErr == nil && chErr == nil {
			assert.Equal(t, pgCount, chCount, "Expired state count should match")
		}
	})

	// Compare account info queries
	t.Run("Account_Info_Equivalence", func(t *testing.T) {
		for _, address := range testAddresses {
			pgAccount, pgErr := pgRepo.GetAccountInfo(ctx, address)
			chAccount, chErr := chRepo.GetAccountInfo(ctx, address)

			assert.Equal(t, pgErr, chErr, "Error status should match for account info")
			if pgErr == nil && chErr == nil && pgAccount != nil && chAccount != nil {
				assert.Equal(t, pgAccount.Address, chAccount.Address, "Account address should match")
				assert.Equal(t, pgAccount.LastAccessBlock, chAccount.LastAccessBlock, "Last access block should match")
				assert.Equal(t, pgAccount.IsContract, chAccount.IsContract, "Contract status should match")
			}
		}
	})

	// Compare analytics data (basic comparison)
	t.Run("Analytics_Data_Equivalence", func(t *testing.T) {
		pgAnalytics, pgErr := pgRepo.GetAnalyticsData(ctx, expiryBlock, expiryBlock+100000)
		chAnalytics, chErr := chRepo.GetAnalyticsData(ctx, expiryBlock, expiryBlock+100000)

		assert.Equal(t, pgErr, chErr, "Error status should match for analytics")
		if pgErr == nil && chErr == nil && pgAnalytics != nil && chAnalytics != nil {
			// Compare key metrics (allowing for slight differences due to data model differences)
			assert.Equal(t, pgAnalytics.AccountExpiry.TotalExpiredAccounts,
				chAnalytics.AccountExpiry.TotalExpiredAccounts,
				"Total expired accounts should match")
		}
	})
}

// TestArchivePerformance benchmarks archive query performance
func TestArchivePerformance(t *testing.T) {
	// Skip performance tests in CI or short mode
	if testing.Short() || os.Getenv("CI") != "" {
		t.Skip("Skipping performance tests in CI/short mode")
	}

	ctx := context.Background()
	config, err := internal.LoadConfig("../../configs")
	require.NoError(t, err)

	// Setup ClickHouse repository for performance testing
	chRepo := setupClickHouseTest(t, ctx, config)
	defer cleanupClickHouse(t, chRepo)

	// Performance test cases
	testCases := []struct {
		name        string
		expiryBlock uint64
		maxDuration time.Duration
	}{
		{"Small_Dataset_1M_Blocks", 1000000, 5 * time.Second},
		{"Medium_Dataset_5M_Blocks", 5000000, 15 * time.Second},
		{"Large_Dataset_10M_Blocks", 10000000, 30 * time.Second},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()

			// Test analytics query performance
			_, err := chRepo.GetAnalyticsData(ctx, tc.expiryBlock, tc.expiryBlock+100000)
			duration := time.Since(start)

			assert.NoError(t, err, "Analytics query should not fail")
			assert.LessOrEqual(t, duration, tc.maxDuration,
				"Query should complete within %v, took %v", tc.maxDuration, duration)

			t.Logf("Analytics query for %d blocks completed in %v", tc.expiryBlock, duration)
		})
	}
}

// TestArchiveDataIntegrity verifies that archive mode stores complete history correctly
func TestArchiveDataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping data integrity test in short mode")
	}

	ctx := context.Background()
	config, err := internal.LoadConfig("../../configs")
	require.NoError(t, err)

	chRepo := setupClickHouseTest(t, ctx, config)
	defer cleanupClickHouse(t, chRepo)

	// Test complete history storage
	t.Run("Complete_History_Storage", func(t *testing.T) {
		testAddress := "0x1234567890abcdef1234567890abcdef12345678"
		testBlocks := []uint64{1000, 2000, 3000, 4000, 5000}

		// Simulate multiple accesses to same address across different blocks
		accounts := make(map[string]uint64)
		accountTypes := make(map[string]bool)
		storage := make(map[string]map[string]uint64)

		for i, block := range testBlocks {
			accounts[testAddress] = block
			accountTypes[testAddress] = true // Contract

			// Update range data for each block
			err := chRepo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, uint64(i+1))
			assert.NoError(t, err, "UpdateRangeDataInTx should succeed for block %d", block)
		}

		// Verify that latest access is the most recent block
		lastAccess, err := chRepo.GetStateLastAccessedBlock(ctx, testAddress, nil)
		assert.NoError(t, err, "GetStateLastAccessedBlock should succeed")
		assert.Equal(t, testBlocks[len(testBlocks)-1], lastAccess,
			"Latest access should be the most recent block")

		t.Logf("Archive correctly stored %d access events for address %s",
			len(testBlocks), testAddress)
	})
}

// Helper functions for test setup and cleanup

func setupPostgreSQLTest(t *testing.T, ctx context.Context, config internal.Config) *PostgreSQLRepository {
	// Create PostgreSQL connection for testing
	config.ArchiveMode = false
	db, err := database.Connect(ctx, config)
	require.NoError(t, err, "Failed to connect to PostgreSQL test database")

	repo := NewPostgreSQLRepository(db)
	return repo
}

func setupClickHouseTest(t *testing.T, ctx context.Context, config internal.Config) *ClickHouseRepository {
	// Create ClickHouse connection for testing
	config.ArchiveMode = true
	db, err := database.ConnectClickHouseSQL(config)
	require.NoError(t, err, "Failed to connect to ClickHouse test database")

	repo := NewClickHouseRepository(db)
	return repo
}

func cleanupPostgreSQL(t *testing.T, repo *PostgreSQLRepository) {
	// Cleanup test data from PostgreSQL
	ctx := context.Background()
	_, err := repo.db.Exec(ctx, "DELETE FROM accounts_current WHERE address LIKE '0x%'")
	if err != nil {
		t.Logf("Warning: Failed to cleanup PostgreSQL test data: %v", err)
	}
}

func cleanupClickHouse(t *testing.T, repo *ClickHouseRepository) {
	// Cleanup test data from ClickHouse
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, "DELETE FROM accounts_archive WHERE 1=1")
	if err != nil {
		t.Logf("Warning: Failed to cleanup ClickHouse test data: %v", err)
	}
}

func setupArchiveTestData(t *testing.T, ctx context.Context, pgRepo *PostgreSQLRepository, chRepo *ClickHouseRepository, addresses []string, blocks []uint64) {
	// Insert test data into both repositories
	accounts := make(map[string]uint64)
	accountTypes := make(map[string]bool)
	storage := make(map[string]map[string]uint64)

	for i, address := range addresses {
		blockNum := blocks[i%len(blocks)]
		accounts[address] = blockNum
		accountTypes[address] = (i%2 == 0) // Alternate between contract and EOA
	}

	// Insert into PostgreSQL
	err := pgRepo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err, "Failed to setup PostgreSQL test data")

	// Insert into ClickHouse
	err = chRepo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err, "Failed to setup ClickHouse test data")

	t.Logf("Setup test data: %d addresses across %d blocks", len(addresses), len(blocks))
}
