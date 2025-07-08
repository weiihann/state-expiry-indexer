package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"
)

// TestData represents common test data structures
type TestData struct {
	Accounts []TestAccount
	Storage  []TestStorage
}

type TestAccount struct {
	Address         string
	LastAccessBlock uint64
	IsContract      bool
}

type TestStorage struct {
	Address         string
	SlotKey         string
	LastAccessBlock uint64
}

// CreateTestData generates realistic test data for database testing
func CreateTestData() TestData {
	return TestData{
		Accounts: []TestAccount{
			{Address: "0x1111111111111111111111111111111111111111", LastAccessBlock: 1000, IsContract: false},
			{Address: "0x2222222222222222222222222222222222222222", LastAccessBlock: 2000, IsContract: true},
			{Address: "0x3333333333333333333333333333333333333333", LastAccessBlock: 3000, IsContract: false},
			{Address: "0x4444444444444444444444444444444444444444", LastAccessBlock: 4000, IsContract: true},
			{Address: "0x5555555555555555555555555555555555555555", LastAccessBlock: 5000, IsContract: false},
		},
		Storage: []TestStorage{
			{Address: "0x2222222222222222222222222222222222222222", SlotKey: "0x0000000000000000000000000000000000000000000000000000000000000001", LastAccessBlock: 2100},
			{Address: "0x2222222222222222222222222222222222222222", SlotKey: "0x0000000000000000000000000000000000000000000000000000000000000002", LastAccessBlock: 2200},
			{Address: "0x4444444444444444444444444444444444444444", SlotKey: "0x0000000000000000000000000000000000000000000000000000000000000001", LastAccessBlock: 4100},
			{Address: "0x4444444444444444444444444444444444444444", SlotKey: "0x0000000000000000000000000000000000000000000000000000000000000003", LastAccessBlock: 4300},
		},
	}
}

// LoadTestData loads test data into the repository
func LoadTestData(t *testing.T, repo StateRepositoryInterface, data TestData) {
	t.Helper()

	ctx := context.Background()

	// Prepare data maps for UpdateRangeDataInTx
	accounts := make(map[string]uint64)
	accountTypes := make(map[string]bool)
	storage := make(map[string]map[string]uint64)

	// Process account data
	for _, account := range data.Accounts {
		accounts[account.Address] = account.LastAccessBlock
		accountTypes[account.Address] = account.IsContract
	}

	// Process storage data
	for _, storageEntry := range data.Storage {
		if storage[storageEntry.Address] == nil {
			storage[storageEntry.Address] = make(map[string]uint64)
		}
		storage[storageEntry.Address][storageEntry.SlotKey] = storageEntry.LastAccessBlock
	}

	// Load data using repository method
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err, "failed to load test data")
}

// AssertAccountExists verifies that an account exists with expected values
func AssertAccountExists(t *testing.T, repo StateRepositoryInterface, address string, expectedBlock uint64, expectedIsContract *bool) {
	t.Helper()

	ctx := context.Background()

	// Cast to PostgreSQL repository to access GetAccountInfo method
	pgRepo, ok := repo.(*PostgreSQLRepository)
	if !ok {
		// For ClickHouse, we'll need to implement this differently
		t.Skip("AssertAccountExists not yet implemented for ClickHouse repository")
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

// AssertStorageExists verifies that a storage slot exists with expected values
func AssertStorageExists(t *testing.T, repo StateRepositoryInterface, address, slot string, expectedBlock uint64) {
	t.Helper()

	ctx := context.Background()

	// Cast to PostgreSQL repository to access GetStateLastAccessedBlock method
	pgRepo, ok := repo.(*PostgreSQLRepository)
	if !ok {
		// For ClickHouse, we'll need to implement this differently
		t.Skip("AssertStorageExists not yet implemented for ClickHouse repository")
		return
	}

	// Check storage access
	lastBlock, err := pgRepo.GetStateLastAccessedBlock(ctx, address, &slot)
	require.NoError(t, err, "failed to get storage last accessed block for %s:%s", address, slot)
	assert.Equal(t, expectedBlock, lastBlock, "last access block mismatch for storage %s:%s", address, slot)
}

// AssertAccountCount verifies the total number of accounts
func AssertAccountCount(t *testing.T, repo StateRepositoryInterface, expiryBlock uint64, expectedEOA, expectedContract int) {
	t.Helper()

	ctx := context.Background()

	// Get analytics data to check account counts
	analytics, err := repo.GetAnalyticsData(ctx, expiryBlock, expiryBlock+1000)
	require.NoError(t, err, "failed to get analytics data")

	assert.Equal(t, expectedEOA, analytics.AccountExpiry.ExpiredEOAs, "expired EOA count mismatch")
	assert.Equal(t, expectedContract, analytics.AccountExpiry.ExpiredContracts, "expired contract count mismatch")
}

// CreateLargeTestData generates a large dataset for performance testing
func CreateLargeTestData(accountCount, storagePerAccount int) TestData {
	data := TestData{
		Accounts: make([]TestAccount, accountCount),
		Storage:  make([]TestStorage, 0, accountCount*storagePerAccount),
	}

	for i := 0; i < accountCount; i++ {
		// Generate test address
		address := generateTestAddress(i)

		data.Accounts[i] = TestAccount{
			Address:         address,
			LastAccessBlock: uint64(1000 + i),
			IsContract:      i%2 == 0, // Alternate between EOA and contract
		}

		// Add storage slots for contracts
		if data.Accounts[i].IsContract {
			for j := 0; j < storagePerAccount; j++ {
				data.Storage = append(data.Storage, TestStorage{
					Address:         address,
					SlotKey:         generateTestSlot(j),
					LastAccessBlock: uint64(1000 + i + j),
				})
			}
		}
	}

	return data
}

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

// SkipIfShort skips the test if running in short mode
func SkipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}
}

// RequireTestDatabase ensures test databases are available
func RequireTestDatabase(t *testing.T, archiveMode bool) {
	t.Helper()

	testConfig := testdb.GetTestConfig()

	if archiveMode {
		// Check ClickHouse availability
		config := createTestConfig(testConfig.ClickHouse, true)
		testdb.WaitForClickHouse(t, config, 5*time.Second)
	} else {
		// Check PostgreSQL availability
		config := createTestConfig(testConfig.PostgreSQL, false)
		testdb.WaitForPostgreSQL(t, config, 5*time.Second)
	}
}

// createTestConfig creates an internal.Config for testing
func createTestConfig(dbConfig testdb.TestDBConfig, archiveMode bool) internal.Config {
	if archiveMode {
		return internal.Config{
			ClickHouseHost:     dbConfig.Host,
			ClickHousePort:     dbConfig.Port,
			ClickHouseUser:     dbConfig.User,
			ClickHousePassword: dbConfig.Password,
			ClickHouseDatabase: dbConfig.Database,
			RPCURLS:            []string{"http://localhost:8545"},
			Environment:        "test",
			ArchiveMode:        true,
		}
	}
	return internal.Config{
		DBHost:      dbConfig.Host,
		DBPort:      dbConfig.Port,
		DBUser:      dbConfig.User,
		DBPassword:  dbConfig.Password,
		DBName:      dbConfig.Database,
		RPCURLS:     []string{"http://localhost:8545"},
		Environment: "test",
		ArchiveMode: false,
	}
}
