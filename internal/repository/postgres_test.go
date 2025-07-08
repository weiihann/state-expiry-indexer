package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Setup test database
	ctx := context.Background()
	db, err := setupTestDB(ctx)
	if err != nil {
		fmt.Printf("Failed to setup test database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Run tests
	code := m.Run()

	// Cleanup
	err = cleanupTestDB(ctx, db)
	if err != nil {
		fmt.Printf("Failed to cleanup test database: %v\n", err)
	}

	os.Exit(code)
}

func setupTestDB(ctx context.Context) (*pgxpool.Pool, error) {
	// Connect to test database
	testDBURL := "postgres://test:test@localhost:15432/test?sslmode=disable"
	config, err := pgxpool.ParseConfig(testDBURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse test database URL: %w", err)
	}

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to test database: %w", err)
	}

	// Create test schema
	err = createTestSchema(ctx, db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create test schema: %w", err)
	}

	return db, nil
}

func createTestSchema(ctx context.Context, db *pgxpool.Pool) error {
	// Create accounts table
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS accounts_current (
			address BYTEA PRIMARY KEY,
			last_access_block BIGINT NOT NULL,
			is_contract BOOLEAN
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create accounts_current table: %w", err)
	}

	// Create storage table
	_, err = db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS storage_current (
			address BYTEA NOT NULL,
			slot_key BYTEA NOT NULL,
			last_access_block BIGINT NOT NULL,
			PRIMARY KEY (address, slot_key)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create storage_current table: %w", err)
	}

	// Create metadata table
	_, err = db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	return nil
}

func cleanupTestDB(ctx context.Context, db *pgxpool.Pool) error {
	// Clean up test data
	_, err := db.Exec(ctx, "TRUNCATE TABLE accounts_current, storage_current, metadata")
	return err
}

func getTestDB(t *testing.T) *pgxpool.Pool {
	ctx := context.Background()
	db, err := setupTestDB(ctx)
	require.NoError(t, err, "Failed to setup test database")
	
	// Clean up before each test
	err = cleanupTestDB(ctx, db)
	require.NoError(t, err, "Failed to cleanup test database")
	
	return db
}

func TestPostgreSQLRepository_GetLastIndexedRange(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("returns 0 when no metadata exists", func(t *testing.T) {
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), range_)
	})

	t.Run("returns correct range when metadata exists", func(t *testing.T) {
		// Insert test metadata
		_, err := db.Exec(ctx, "INSERT INTO metadata (key, value) VALUES ('last_indexed_range', '123')")
		require.NoError(t, err)

		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(123), range_)
	})
}

func TestPostgreSQLRepository_UpdateRangeDataInTx(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("successfully updates accounts, storage, and range", func(t *testing.T) {
		// Test data
		accounts := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 100,
			"0xabcdef1234567890abcdef1234567890abcdef12": 101,
		}
		accountTypes := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false, // EOA
			"0xabcdef1234567890abcdef1234567890abcdef12": true,  // Contract
		}
		storage := map[string]map[string]uint64{
			"0xabcdef1234567890abcdef1234567890abcdef12": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 102,
				"0x0000000000000000000000000000000000000000000000000000000000000002": 103,
			},
		}
		rangeNumber := uint64(5)

		// Execute update
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, rangeNumber)
		assert.NoError(t, err)

		// Verify accounts were inserted
		var count int
		err = db.QueryRow(ctx, "SELECT COUNT(*) FROM accounts_current").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// Verify storage was inserted
		err = db.QueryRow(ctx, "SELECT COUNT(*) FROM storage_current").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// Verify range was updated
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, rangeNumber, range_)
	})

	t.Run("handles empty data gracefully", func(t *testing.T) {
		accounts := map[string]uint64{}
		accountTypes := map[string]bool{}
		storage := map[string]map[string]uint64{}
		rangeNumber := uint64(10)

		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, rangeNumber)
		assert.NoError(t, err)

		// Verify range was still updated
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, rangeNumber, range_)
	})

	t.Run("updates existing accounts with newer blocks", func(t *testing.T) {
		// Insert initial account
		accounts1 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 100,
		}
		accountTypes1 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err := repo.UpdateRangeDataInTx(ctx, accounts1, accountTypes1, nil, 1)
		assert.NoError(t, err)

		// Update with newer block
		accounts2 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 200,
		}
		accountTypes2 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err = repo.UpdateRangeDataInTx(ctx, accounts2, accountTypes2, nil, 2)
		assert.NoError(t, err)

		// Verify account was updated
		lastBlock, err := repo.GetStateLastAccessedBlock(ctx, "0x1234567890abcdef1234567890abcdef12345678", nil)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), lastBlock)
	})

	t.Run("ignores older blocks", func(t *testing.T) {
		// Insert account with block 200
		accounts1 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 200,
		}
		accountTypes1 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err := repo.UpdateRangeDataInTx(ctx, accounts1, accountTypes1, nil, 1)
		assert.NoError(t, err)

		// Try to update with older block
		accounts2 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 100,
		}
		accountTypes2 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err = repo.UpdateRangeDataInTx(ctx, accounts2, accountTypes2, nil, 2)
		assert.NoError(t, err)

		// Verify account was not updated
		lastBlock, err := repo.GetStateLastAccessedBlock(ctx, "0x1234567890abcdef1234567890abcdef12345678", nil)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), lastBlock)
	})
}

func TestPostgreSQLRepository_GetStateLastAccessedBlock(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data
	accounts := map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": 100,
	}
	accountTypes := map[string]bool{
		"0x1234567890abcdef1234567890abcdef12345678": true,
	}
	storage := map[string]map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 200,
		},
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err)

	t.Run("returns correct block for account", func(t *testing.T) {
		block, err := repo.GetStateLastAccessedBlock(ctx, "0x1234567890abcdef1234567890abcdef12345678", nil)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), block)
	})

	t.Run("returns correct block for storage slot", func(t *testing.T) {
		slot := "0x0000000000000000000000000000000000000000000000000000000000000001"
		block, err := repo.GetStateLastAccessedBlock(ctx, "0x1234567890abcdef1234567890abcdef12345678", &slot)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), block)
	})

	t.Run("returns 0 for non-existent account", func(t *testing.T) {
		block, err := repo.GetStateLastAccessedBlock(ctx, "0x0000000000000000000000000000000000000000", nil)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), block)
	})

	t.Run("returns 0 for non-existent storage slot", func(t *testing.T) {
		slot := "0x0000000000000000000000000000000000000000000000000000000000000999"
		block, err := repo.GetStateLastAccessedBlock(ctx, "0x1234567890abcdef1234567890abcdef12345678", &slot)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), block)
	})

	t.Run("handles invalid hex addresses", func(t *testing.T) {
		_, err := repo.GetStateLastAccessedBlock(ctx, "invalid_hex", nil)
		assert.Error(t, err)
	})
}

func TestPostgreSQLRepository_GetAccountType(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data
	accounts := map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": 100, // EOA
		"0xabcdef1234567890abcdef1234567890abcdef12": 101, // Contract
	}
	accountTypes := map[string]bool{
		"0x1234567890abcdef1234567890abcdef12345678": false, // EOA
		"0xabcdef1234567890abcdef1234567890abcdef12": true,  // Contract
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
	require.NoError(t, err)

	t.Run("returns false for EOA", func(t *testing.T) {
		isContract, err := repo.GetAccountType(ctx, "0x1234567890abcdef1234567890abcdef12345678")
		assert.NoError(t, err)
		assert.NotNil(t, isContract)
		assert.False(t, *isContract)
	})

	t.Run("returns true for contract", func(t *testing.T) {
		isContract, err := repo.GetAccountType(ctx, "0xabcdef1234567890abcdef1234567890abcdef12")
		assert.NoError(t, err)
		assert.NotNil(t, isContract)
		assert.True(t, *isContract)
	})

	t.Run("returns nil for non-existent account", func(t *testing.T) {
		isContract, err := repo.GetAccountType(ctx, "0x0000000000000000000000000000000000000000")
		assert.NoError(t, err)
		assert.Nil(t, isContract)
	})
}

func TestPostgreSQLRepository_GetAccountInfo(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data
	accounts := map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": 100,
	}
	accountTypes := map[string]bool{
		"0x1234567890abcdef1234567890abcdef12345678": true,
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
	require.NoError(t, err)

	t.Run("returns correct account info", func(t *testing.T) {
		account, err := repo.GetAccountInfo(ctx, "0x1234567890abcdef1234567890abcdef12345678")
		assert.NoError(t, err)
		assert.NotNil(t, account)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", account.Address)
		assert.Equal(t, uint64(100), account.LastAccessBlock)
		assert.NotNil(t, account.IsContract)
		assert.True(t, *account.IsContract)
	})

	t.Run("returns nil for non-existent account", func(t *testing.T) {
		account, err := repo.GetAccountInfo(ctx, "0x0000000000000000000000000000000000000000")
		assert.NoError(t, err)
		assert.Nil(t, account)
	})
}

func TestPostgreSQLRepository_GetExpiredStateCount(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data
	accounts := map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": 50,  // Expired (< 100)
		"0xabcdef1234567890abcdef1234567890abcdef12": 150, // Not expired (>= 100)
	}
	accountTypes := map[string]bool{
		"0x1234567890abcdef1234567890abcdef12345678": false,
		"0xabcdef1234567890abcdef1234567890abcdef12": true,
	}
	storage := map[string]map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50,  // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000002": 150, // Not expired
		},
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err)

	t.Run("returns correct expired count", func(t *testing.T) {
		count, err := repo.GetExpiredStateCount(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, 2, count) // 1 expired account + 1 expired storage slot
	})
}

func TestPostgreSQLRepository_GetTopNExpiredContracts(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data with multiple contracts
	storage := map[string]map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50, // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000002": 50, // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000003": 50, // Expired
		},
		"0xabcdef1234567890abcdef1234567890abcdef12": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50, // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000002": 50, // Expired
		},
		"0x1111111111111111111111111111111111111111": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50, // Expired
		},
	}
	err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, 1)
	require.NoError(t, err)

	t.Run("returns top N expired contracts", func(t *testing.T) {
		contracts, err := repo.GetTopNExpiredContracts(ctx, 100, 2)
		assert.NoError(t, err)
		assert.Len(t, contracts, 2)
		
		// Should be ordered by slot count descending
		assert.Equal(t, 3, contracts[0].SlotCount)
		assert.Equal(t, 2, contracts[1].SlotCount)
	})

	t.Run("returns empty list when no expired contracts", func(t *testing.T) {
		contracts, err := repo.GetTopNExpiredContracts(ctx, 10, 10) // Very low expiry block
		assert.NoError(t, err)
		assert.Len(t, contracts, 0)
	})
}

func TestPostgreSQLRepository_GetExpiredAccountsByType(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup test data
	accounts := map[string]uint64{
		"0x1234567890abcdef1234567890abcdef12345678": 50,  // Expired EOA
		"0xabcdef1234567890abcdef1234567890abcdef12": 50,  // Expired Contract
		"0x2222222222222222222222222222222222222222": 150, // Not expired EOA
		"0x3333333333333333333333333333333333333333": 150, // Not expired Contract
	}
	accountTypes := map[string]bool{
		"0x1234567890abcdef1234567890abcdef12345678": false, // EOA
		"0xabcdef1234567890abcdef1234567890abcdef12": true,  // Contract
		"0x2222222222222222222222222222222222222222": false, // EOA
		"0x3333333333333333333333333333333333333333": true,  // Contract
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
	require.NoError(t, err)

	t.Run("returns all expired accounts", func(t *testing.T) {
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, nil)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
	})

	t.Run("returns only expired EOAs", func(t *testing.T) {
		isContract := false
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, &isContract)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.NotNil(t, accounts[0].IsContract)
		assert.False(t, *accounts[0].IsContract)
	})

	t.Run("returns only expired contracts", func(t *testing.T) {
		isContract := true
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, &isContract)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.NotNil(t, accounts[0].IsContract)
		assert.True(t, *accounts[0].IsContract)
	})
}

func TestPostgreSQLRepository_GetSyncStatus(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("returns correct sync status when synced", func(t *testing.T) {
		// Set last indexed range to 10
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, nil, 10)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 10, 1000)
		assert.NoError(t, err)
		assert.True(t, status.IsSynced)
		assert.Equal(t, uint64(10), status.LastIndexedRange)
		assert.Equal(t, uint64(10000), status.EndBlock) // 10 * 1000
	})

	t.Run("returns correct sync status when not synced", func(t *testing.T) {
		// Set last indexed range to 5
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, nil, 5)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 10, 1000)
		assert.NoError(t, err)
		assert.False(t, status.IsSynced)
		assert.Equal(t, uint64(5), status.LastIndexedRange)
		assert.Equal(t, uint64(5000), status.EndBlock) // 5 * 1000
	})

	t.Run("handles genesis range correctly", func(t *testing.T) {
		// Clean db (no metadata)
		err := cleanupTestDB(ctx, db)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 1, 1000)
		assert.NoError(t, err)
		assert.False(t, status.IsSynced)
		assert.Equal(t, uint64(0), status.LastIndexedRange)
		assert.Equal(t, uint64(0), status.EndBlock)
	})
}

func TestPostgreSQLRepository_GetAnalyticsData(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	// Setup comprehensive test data
	accounts := map[string]uint64{
		"0x1111111111111111111111111111111111111111": 50,  // Expired EOA
		"0x2222222222222222222222222222222222222222": 50,  // Expired EOA
		"0x3333333333333333333333333333333333333333": 50,  // Expired Contract
		"0x4444444444444444444444444444444444444444": 150, // Active EOA
		"0x5555555555555555555555555555555555555555": 150, // Active Contract
	}
	accountTypes := map[string]bool{
		"0x1111111111111111111111111111111111111111": false, // EOA
		"0x2222222222222222222222222222222222222222": false, // EOA
		"0x3333333333333333333333333333333333333333": true,  // Contract
		"0x4444444444444444444444444444444444444444": false, // EOA
		"0x5555555555555555555555555555555555555555": true,  // Contract
	}
	storage := map[string]map[string]uint64{
		"0x3333333333333333333333333333333333333333": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50,  // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000002": 50,  // Expired
		},
		"0x5555555555555555555555555555555555555555": {
			"0x0000000000000000000000000000000000000000000000000000000000000001": 50,  // Expired
			"0x0000000000000000000000000000000000000000000000000000000000000002": 150, // Active
		},
	}
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, storage, 1)
	require.NoError(t, err)

	t.Run("returns comprehensive analytics data", func(t *testing.T) {
		analytics, err := repo.GetAnalyticsData(ctx, 100, 200)
		assert.NoError(t, err)
		assert.NotNil(t, analytics)

		// Test Account Expiry Analysis
		assert.Equal(t, 2, analytics.AccountExpiry.ExpiredEOAs)
		assert.Equal(t, 1, analytics.AccountExpiry.ExpiredContracts)
		assert.Equal(t, 3, analytics.AccountExpiry.TotalExpiredAccounts)
		assert.Equal(t, 3, analytics.AccountExpiry.TotalEOAs)
		assert.Equal(t, 2, analytics.AccountExpiry.TotalContracts)
		assert.Equal(t, 5, analytics.AccountExpiry.TotalAccounts)

		// Test percentages
		assert.InDelta(t, 66.67, analytics.AccountExpiry.ExpiredEOAPercentage, 0.1)
		assert.InDelta(t, 50.0, analytics.AccountExpiry.ExpiredContractPercentage, 0.1)
		assert.InDelta(t, 60.0, analytics.AccountExpiry.TotalExpiredPercentage, 0.1)

		// Test Account Distribution Analysis
		assert.InDelta(t, 33.33, analytics.AccountDistribution.ContractPercentage, 0.1)
		assert.InDelta(t, 66.67, analytics.AccountDistribution.EOAPercentage, 0.1)
		assert.Equal(t, 3, analytics.AccountDistribution.TotalExpiredAccounts)

		// Test Storage Slot Expiry Analysis
		assert.Equal(t, 3, analytics.StorageSlotExpiry.ExpiredSlots)
		assert.Equal(t, 4, analytics.StorageSlotExpiry.TotalSlots)
		assert.InDelta(t, 75.0, analytics.StorageSlotExpiry.ExpiredSlotPercentage, 0.1)

		// Test Contract Storage Analysis
		assert.NotNil(t, analytics.ContractStorage.TopExpiredContracts)
		
		// Test Storage Expiry Analysis
		assert.Greater(t, analytics.StorageExpiry.ContractsAnalyzed, 0)
		
		// Test Fully Expired Contracts Analysis
		assert.NotNil(t, analytics.FullyExpiredContracts)
		
		// Test Complete Expiry Analysis
		assert.NotNil(t, analytics.CompleteExpiry)
	})
}

func TestPostgreSQLRepository_ConcurrentOperations(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("handles concurrent writes correctly", func(t *testing.T) {
		// Run multiple concurrent operations
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(i int) {
				accounts := map[string]uint64{
					fmt.Sprintf("0x%040d", i): uint64(100 + i),
				}
				accountTypes := map[string]bool{
					fmt.Sprintf("0x%040d", i): i%2 == 0,
				}
				err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, uint64(i))
				assert.NoError(t, err)
				done <- true
			}(i)
		}

		// Wait for all operations to complete
		for i := 0; i < 10; i++ {
			select {
			case <-done:
				continue
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for concurrent operations")
			}
		}

		// Verify all accounts were inserted
		var count int
		err := db.QueryRow(ctx, "SELECT COUNT(*) FROM accounts_current").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 10, count)
	})
}

func TestPostgreSQLRepository_ErrorHandling(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("handles invalid hex addresses gracefully", func(t *testing.T) {
		accounts := map[string]uint64{
			"invalid_hex": 100,
		}
		accountTypes := map[string]bool{
			"invalid_hex": false,
		}
		
		// This should not fail the entire transaction but should skip invalid addresses
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
		assert.NoError(t, err)
		
		// Verify the range was still updated
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), range_)
	})

	t.Run("handles invalid hex in storage gracefully", func(t *testing.T) {
		storage := map[string]map[string]uint64{
			"invalid_address": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 100,
			},
			"0x1234567890abcdef1234567890abcdef12345678": {
				"invalid_slot": 100,
			},
		}
		
		// This should not fail the entire transaction but should skip invalid hex
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, 2)
		assert.NoError(t, err)
		
		// Verify the range was still updated
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), range_)
	})
}

func TestPostgreSQLRepository_LargeBatchProcessing(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	repo := NewPostgreSQLRepository(db)
	ctx := context.Background()

	t.Run("processes large batches efficiently", func(t *testing.T) {
		// Create a large batch of accounts (more than batchSize)
		accounts := make(map[string]uint64)
		accountTypes := make(map[string]bool)
		
		for i := 0; i < 25000; i++ { // More than batchSize (21845)
			addr := fmt.Sprintf("0x%040d", i)
			accounts[addr] = uint64(100 + i)
			accountTypes[addr] = i%2 == 0
		}
		
		start := time.Now()
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Less(t, duration, 30*time.Second, "Large batch processing should complete within 30 seconds")
		
		// Verify all accounts were inserted
		var count int
		err = db.QueryRow(ctx, "SELECT COUNT(*) FROM accounts_current").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 25000, count)
	})
}