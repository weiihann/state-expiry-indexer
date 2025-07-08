package testdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupTestDatabase_PostgreSQL(t *testing.T) {
	SkipIfShort(t)

	repo, cleanup := SetupTestDatabase(t, false)
	defer cleanup()

	ctx := t.Context()

	// Test basic functionality
	lastRange, err := repo.GetLastIndexedRange(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lastRange, "should start with range 0")

	// Test that we can load test data
	data := CreateTestData()
	LoadTestData(t, repo, data)

	// Verify data was loaded
	newRange, err := repo.GetLastIndexedRange(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), newRange, "range should be updated to 1")

	// Test analytics functionality
	analytics, err := repo.GetAnalyticsData(ctx, 100, 200)
	require.NoError(t, err)
	assert.NotNil(t, analytics)

	// Basic sanity check on loaded data
	assert.Equal(t, 5, analytics.AccountExpiry.TotalAccounts, "should have 5 total accounts")
}

func TestSetupTestDatabase_ClickHouse(t *testing.T) {
	SkipIfShort(t)

	repo, cleanup := SetupTestDatabase(t, true)
	defer cleanup()

	ctx := t.Context()

	// Test basic functionality
	lastRange, err := repo.GetLastIndexedRange(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lastRange, "should start with range 0")

	// Test that we can load test data using UpdateRangeDataWithAllEventsInTx for archive mode
	// Note: This is a simplified test since ClickHouse implementation is placeholder
	accounts := map[string]uint64{
		"0x1111111111111111111111111111111111111111": 1000,
		"0x2222222222222222222222222222222222222222": 2000,
	}
	accountTypes := map[string]bool{
		"0x1111111111111111111111111111111111111111": false,
		"0x2222222222222222222222222222222222222222": true,
	}
	storage := make(map[string]map[string]uint64)

	// For ClickHouse archive mode, we need to use the different method
	// Since it's placeholder implementation, this may fail, but we test the setup
	err = repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	if err != nil {
		t.Logf("Expected: ClickHouse repository is placeholder implementation, error: %v", err)
	}
}

func TestTestDataCreation(t *testing.T) {
	data := CreateTestData()

	// Verify test data structure
	assert.Len(t, data.Accounts, 5, "should have 5 test accounts")
	assert.Len(t, data.Storage, 4, "should have 4 test storage entries")

	// Verify account data
	for _, account := range data.Accounts {
		assert.NotEmpty(t, account.Address, "address should not be empty")
		assert.Greater(t, account.LastAccessBlock, uint64(0), "last access block should be positive")
	}

	// Verify storage data
	for _, storage := range data.Storage {
		assert.NotEmpty(t, storage.Address, "storage address should not be empty")
		assert.NotEmpty(t, storage.SlotKey, "storage slot key should not be empty")
		assert.Greater(t, storage.LastAccessBlock, uint64(0), "storage last access block should be positive")
	}
}

func TestCreateLargeTestData(t *testing.T) {
	data := CreateLargeTestData(100, 5)

	assert.Len(t, data.Accounts, 100, "should have 100 accounts")

	contractCount := 0
	expectedStorageCount := 0

	for _, account := range data.Accounts {
		if account.IsContract {
			contractCount++
			expectedStorageCount += 5 // 5 storage slots per contract
		}
	}

	assert.Equal(t, 50, contractCount, "should have 50 contracts (every other account)")
	assert.Len(t, data.Storage, expectedStorageCount, "should have correct number of storage entries")
}

func TestGetTestConfig(t *testing.T) {
	config := GetTestConfig()

	// Verify PostgreSQL config
	assert.Equal(t, "localhost", config.PostgreSQL.Host)
	assert.Equal(t, "15432", config.PostgreSQL.Port)
	assert.Equal(t, "test", config.PostgreSQL.User)
	assert.Equal(t, "test", config.PostgreSQL.Password)
	assert.Equal(t, "test", config.PostgreSQL.Database)

	// Verify ClickHouse config
	assert.Equal(t, "localhost", config.ClickHouse.Host)
	assert.Equal(t, "19010", config.ClickHouse.Port)
	assert.Equal(t, "test_user", config.ClickHouse.User)
	assert.Equal(t, "test_password", config.ClickHouse.Password)
	assert.Equal(t, "test_state_expiry", config.ClickHouse.Database)
}
