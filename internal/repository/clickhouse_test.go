package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseMain(t *testing.T) {
	// This is a placeholder test to ensure the setup functions work
	ctx := context.Background()
	db, err := setupTestClickHouseDB(ctx)
	if err != nil {
		t.Fatalf("Failed to setup test ClickHouse database: %v", err)
	}
	defer db.Close()

	// Test that we can connect and the schema is created
	err = db.PingContext(ctx)
	if err != nil {
		t.Fatalf("Failed to ping ClickHouse database: %v", err)
	}

	// Test that tables exist
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query accounts_archive table: %v", err)
	}

	// Cleanup
	err = cleanupTestClickHouseDB(ctx, db)
	if err != nil {
		t.Fatalf("Failed to cleanup test ClickHouse database: %v", err)
	}
}

func setupTestClickHouseDB(ctx context.Context) (*sql.DB, error) {
	// Connect to test ClickHouse database
	testDBURL := "http://test_user:test_password@localhost:18123/test_state_expiry"
	db, err := sql.Open("clickhouse", testDBURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to test ClickHouse database: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping test ClickHouse database: %w", err)
	}

	// Create test schema
	err = createTestClickHouseSchema(ctx, db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create test ClickHouse schema: %w", err)
	}

	return db, nil
}

func createTestClickHouseSchema(ctx context.Context, db *sql.DB) error {
	// Create accounts_archive table
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS accounts_archive (
			address FixedString(20),
			block_number UInt64,
			is_contract UInt8
		) ENGINE = MergeTree()
		ORDER BY (address, block_number)
	`)
	if err != nil {
		return fmt.Errorf("failed to create accounts_archive table: %w", err)
	}

	// Create storage_archive table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS storage_archive (
			address FixedString(20),
			slot_key FixedString(32),
			block_number UInt64
		) ENGINE = MergeTree()
		ORDER BY (address, slot_key, block_number)
	`)
	if err != nil {
		return fmt.Errorf("failed to create storage_archive table: %w", err)
	}

	// Create metadata_archive table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS metadata_archive (
			key String,
			value String
		) ENGINE = ReplacingMergeTree()
		ORDER BY key
	`)
	if err != nil {
		return fmt.Errorf("failed to create metadata_archive table: %w", err)
	}

	// Create latest_account_access view
	_, err = db.ExecContext(ctx, `
		CREATE VIEW IF NOT EXISTS latest_account_access AS
		SELECT 
			address,
			argMax(block_number, block_number) as last_access_block,
			argMax(is_contract, block_number) as is_contract
		FROM accounts_archive
		GROUP BY address
	`)
	if err != nil {
		return fmt.Errorf("failed to create latest_account_access view: %w", err)
	}

	// Create latest_storage_access view
	_, err = db.ExecContext(ctx, `
		CREATE VIEW IF NOT EXISTS latest_storage_access AS
		SELECT 
			address,
			slot_key,
			argMax(block_number, block_number) as last_access_block
		FROM storage_archive
		GROUP BY address, slot_key
	`)
	if err != nil {
		return fmt.Errorf("failed to create latest_storage_access view: %w", err)
	}

	return nil
}

func cleanupTestClickHouseDB(ctx context.Context, db *sql.DB) error {
	// Clean up test data
	_, err := db.ExecContext(ctx, "TRUNCATE TABLE accounts_archive")
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "TRUNCATE TABLE storage_archive")
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "TRUNCATE TABLE metadata_archive")
	return err
}

func getTestClickHouseDB(t *testing.T) *sql.DB {
	ctx := context.Background()
	db, err := setupTestClickHouseDB(ctx)
	require.NoError(t, err, "Failed to setup test ClickHouse database")
	
	// Clean up before each test
	err = cleanupTestClickHouseDB(ctx, db)
	require.NoError(t, err, "Failed to cleanup test ClickHouse database")
	
	return db
}

func TestClickHouseRepository_GetLastIndexedRange(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
	ctx := context.Background()

	t.Run("returns 0 when no metadata exists", func(t *testing.T) {
		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), range_)
	})

	t.Run("returns correct range when metadata exists", func(t *testing.T) {
		// Insert test metadata
		_, err := db.ExecContext(ctx, "INSERT INTO metadata_archive (key, value) VALUES (?, ?)", "last_indexed_range", "123")
		require.NoError(t, err)

		range_, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(123), range_)
	})
}

func TestClickHouseRepository_UpdateRangeDataInTx(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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

		// Wait for ClickHouse to process the data
		time.Sleep(100 * time.Millisecond)

		// Verify accounts were inserted
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// Verify storage was inserted
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM storage_archive").Scan(&count)
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

	t.Run("stores all access events in archive mode", func(t *testing.T) {
		// Insert same account multiple times with different blocks
		accounts1 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 100,
		}
		accountTypes1 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err := repo.UpdateRangeDataInTx(ctx, accounts1, accountTypes1, nil, 1)
		assert.NoError(t, err)

		// Insert same account with different block
		accounts2 := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 200,
		}
		accountTypes2 := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err = repo.UpdateRangeDataInTx(ctx, accounts2, accountTypes2, nil, 2)
		assert.NoError(t, err)

		// Wait for ClickHouse to process the data
		time.Sleep(100 * time.Millisecond)

		// Verify both events are stored (archive mode)
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive WHERE address = unhex('1234567890abcdef1234567890abcdef12345678')").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count) // Both events should be stored
	})
}

func TestClickHouseRepository_GetExpiredStateCount(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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
	err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
	require.NoError(t, err)

	// Wait for ClickHouse to process the data
	time.Sleep(100 * time.Millisecond)

	t.Run("returns correct expired count", func(t *testing.T) {
		count, err := repo.GetExpiredStateCount(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, 1, count) // 1 expired account
	})
}

func TestClickHouseRepository_GetTopNExpiredContracts(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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

	// Wait for ClickHouse to process the data
	time.Sleep(100 * time.Millisecond)

	t.Run("returns top N expired contracts", func(t *testing.T) {
		contracts, err := repo.GetTopNExpiredContracts(ctx, 100, 2)
		assert.NoError(t, err)
		assert.Len(t, contracts, 2)
		
		// Should be ordered by slot count descending
		assert.Equal(t, 3, contracts[0].SlotCount)
		assert.Equal(t, 2, contracts[1].SlotCount)
		
		// Verify addresses have 0x prefix
		assert.True(t, contracts[0].Address[:2] == "0x")
		assert.True(t, contracts[1].Address[:2] == "0x")
	})

	t.Run("returns empty list when no expired contracts", func(t *testing.T) {
		contracts, err := repo.GetTopNExpiredContracts(ctx, 10, 10) // Very low expiry block
		assert.NoError(t, err)
		assert.Len(t, contracts, 0)
	})
}

func TestClickHouseRepository_GetExpiredAccountsByType(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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

	// Wait for ClickHouse to process the data
	time.Sleep(100 * time.Millisecond)

	t.Run("returns all expired accounts", func(t *testing.T) {
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, nil)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		
		// Verify addresses have 0x prefix
		for _, account := range accounts {
			assert.True(t, account.Address[:2] == "0x")
		}
	})

	t.Run("returns only expired EOAs", func(t *testing.T) {
		isContract := false
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, &isContract)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.NotNil(t, accounts[0].IsContract)
		assert.False(t, *accounts[0].IsContract)
		assert.True(t, accounts[0].Address[:2] == "0x")
	})

	t.Run("returns only expired contracts", func(t *testing.T) {
		isContract := true
		accounts, err := repo.GetExpiredAccountsByType(ctx, 100, &isContract)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.NotNil(t, accounts[0].IsContract)
		assert.True(t, *accounts[0].IsContract)
		assert.True(t, accounts[0].Address[:2] == "0x")
	})
}

func TestClickHouseRepository_GetSyncStatus(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
	ctx := context.Background()

	t.Run("returns correct sync status when synced", func(t *testing.T) {
		// Set last indexed range to 10
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, nil, 10)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 10, 1000)
		assert.NoError(t, err)
		assert.True(t, status.IsSynced)
		assert.Equal(t, uint64(10), status.LastIndexedRange)
		assert.Equal(t, uint64(11000), status.EndBlock) // (10 + 1) * 1000
	})

	t.Run("returns correct sync status when not synced", func(t *testing.T) {
		// Set last indexed range to 5
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, nil, 5)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 10, 1000)
		assert.NoError(t, err)
		assert.False(t, status.IsSynced)
		assert.Equal(t, uint64(5), status.LastIndexedRange)
		assert.Equal(t, uint64(6000), status.EndBlock) // (5 + 1) * 1000
	})

	t.Run("handles genesis range correctly", func(t *testing.T) {
		// Clean db (no metadata)
		err := cleanupTestClickHouseDB(ctx, db)
		require.NoError(t, err)

		status, err := repo.GetSyncStatus(ctx, 1, 1000)
		assert.NoError(t, err)
		assert.False(t, status.IsSynced)
		assert.Equal(t, uint64(0), status.LastIndexedRange)
		assert.Equal(t, uint64(1000), status.EndBlock) // (0 + 1) * 1000
	})
}

func TestClickHouseRepository_GetAnalyticsData(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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

	// Wait for ClickHouse to process the data
	time.Sleep(200 * time.Millisecond)

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

func TestClickHouseRepository_ErrorHandling(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
	ctx := context.Background()

	t.Run("handles invalid hex addresses gracefully", func(t *testing.T) {
		accounts := map[string]uint64{
			"invalid_hex": 100,
		}
		accountTypes := map[string]bool{
			"invalid_hex": false,
		}
		
		// This should fail due to invalid hex
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid address length")
	})

	t.Run("handles invalid hex in storage gracefully", func(t *testing.T) {
		storage := map[string]map[string]uint64{
			"invalid_address": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": 100,
			},
		}
		
		// This should fail due to invalid address hex
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, 2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid address length")
	})

	t.Run("handles invalid slot hex gracefully", func(t *testing.T) {
		storage := map[string]map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": {
				"invalid_slot": 100,
			},
		}
		
		// This should fail due to invalid slot hex
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, 3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid slot length")
	})

	t.Run("handles wrong hex length in addresses", func(t *testing.T) {
		accounts := map[string]uint64{
			"0x1234": 100, // Too short
		}
		accountTypes := map[string]bool{
			"0x1234": false,
		}
		
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid address length")
	})

	t.Run("handles wrong hex length in slots", func(t *testing.T) {
		storage := map[string]map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": {
				"0x1234": 100, // Too short for slot
			},
		}
		
		err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, 5)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid slot length")
	})
}

func TestClickHouseRepository_ArchiveBehavior(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
	ctx := context.Background()

	t.Run("stores multiple events for same account", func(t *testing.T) {
		// Insert same account at different blocks
		for i := 1; i <= 5; i++ {
			accounts := map[string]uint64{
				"0x1234567890abcdef1234567890abcdef12345678": uint64(i * 100),
			}
			accountTypes := map[string]bool{
				"0x1234567890abcdef1234567890abcdef12345678": false,
			}
			err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, uint64(i))
			assert.NoError(t, err)
		}

		// Wait for ClickHouse to process the data
		time.Sleep(200 * time.Millisecond)

		// Verify all 5 events are stored
		var count int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive WHERE address = unhex('1234567890abcdef1234567890abcdef12345678')").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("stores multiple events for same storage slot", func(t *testing.T) {
		// Insert same storage slot at different blocks
		for i := 1; i <= 3; i++ {
			storage := map[string]map[string]uint64{
				"0x1234567890abcdef1234567890abcdef12345678": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": uint64(i * 50),
				},
			}
			err := repo.UpdateRangeDataInTx(ctx, nil, nil, storage, uint64(i))
			assert.NoError(t, err)
		}

		// Wait for ClickHouse to process the data
		time.Sleep(200 * time.Millisecond)

		// Verify all 3 events are stored
		var count int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM storage_archive 
			WHERE address = unhex('1234567890abcdef1234567890abcdef12345678') 
			AND slot_key = unhex('0000000000000000000000000000000000000000000000000000000000000001')
		`).Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("latest views return most recent access", func(t *testing.T) {
		// Insert account at multiple blocks
		accounts := map[string]uint64{
			"0x1234567890abcdef1234567890abcdef12345678": 100,
		}
		accountTypes := map[string]bool{
			"0x1234567890abcdef1234567890abcdef12345678": false,
		}
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
		assert.NoError(t, err)

		// Update with newer block
		accounts["0x1234567890abcdef1234567890abcdef12345678"] = 200
		err = repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 2)
		assert.NoError(t, err)

		// Wait for ClickHouse to process the data
		time.Sleep(200 * time.Millisecond)

		// Verify latest view returns most recent block
		var lastBlock uint64
		err = db.QueryRowContext(ctx, `
			SELECT last_access_block FROM latest_account_access 
			WHERE address = unhex('1234567890abcdef1234567890abcdef12345678')
		`).Scan(&lastBlock)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), lastBlock)
	})
}

func TestClickHouseRepository_ConcurrentOperations(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
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
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for concurrent operations")
			}
		}

		// Wait for ClickHouse to process all data
		time.Sleep(500 * time.Millisecond)

		// Verify all accounts were inserted
		var count int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 10, count)
	})
}

func TestClickHouseRepository_LargeBatchProcessing(t *testing.T) {
	db := getTestClickHouseDB(t)
	defer db.Close()

	repo := NewClickHouseRepository(db)
	ctx := context.Background()

	t.Run("processes large batches efficiently", func(t *testing.T) {
		// Create a large batch of accounts
		accounts := make(map[string]uint64)
		accountTypes := make(map[string]bool)
		
		for i := 0; i < 1000; i++ {
			addr := fmt.Sprintf("0x%040d", i)
			accounts[addr] = uint64(100 + i)
			accountTypes[addr] = i%2 == 0
		}
		
		start := time.Now()
		err := repo.UpdateRangeDataInTx(ctx, accounts, accountTypes, nil, 1)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Less(t, duration, 10*time.Second, "Large batch processing should complete within 10 seconds")
		
		// Wait for ClickHouse to process all data
		time.Sleep(500 * time.Millisecond)
		
		// Verify all accounts were inserted
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts_archive").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1000, count)
	})
}