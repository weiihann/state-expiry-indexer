package indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
)

// TestRangeProcessing tests the complete range processing workflow
func TestRangeProcessing(t *testing.T) {
	tests := []struct {
		name        string
		archiveMode bool
		rangeSize   int
	}{
		{
			name:        "PostgreSQL Range Processing",
			archiveMode: false,
			rangeSize:   100,
		},
		{
			name:        "ClickHouse Range Processing",
			archiveMode: true,
			rangeSize:   100,
		},
		{
			name:        "Large Range Processing",
			archiveMode: true,
			rangeSize:   1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test environment
			dataDir, cleanupDir := createTestDataDir(t)
			defer cleanupDir()

			// Create test configuration
			config := createTestConfig(tt.archiveMode, dataDir)
			config.RangeSize = tt.rangeSize

			// Setup database
			repo, cleanupDB := createTestRepository(t, tt.archiveMode, config)
			defer cleanupDB()

			// Create mock RPC client with test data
			mockRPC := NewMockRPCClient()
			mockRPC.SetLatestBlock(uint64(tt.rangeSize * 2)) // Enough blocks for 2 ranges

			// Create range processor
			rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, tt.rangeSize)
			require.NoError(t, err, "Failed to create range processor")
			defer rangeProcessor.Close()

			// Create indexer with range processor
			indexer := NewIndexer(repo, rangeProcessor, mockRPC, config)
			ctx := context.Background()

			// Test range processing workflow
			t.Run("ProcessGenesis", func(t *testing.T) {
				err := indexer.ProcessGenesis(ctx)
				assert.NoError(t, err, "Genesis processing should succeed")

				// Verify genesis was processed
				lastRange, err := repo.GetLastIndexedRange(ctx)
				assert.NoError(t, err, "Should be able to get last indexed range")
				assert.Equal(t, uint64(0), lastRange, "Genesis should be processed")
			})

			t.Run("ProcessRange", func(t *testing.T) {
				// Create appropriate state access
				var sa StateAccess
				if tt.archiveMode {
					sa = newStateAccessArchive()
				} else {
					sa = newStateAccessLatest()
				}

				// Process range 1
				err := indexer.ProcessRange(ctx, 1, sa, true)
				assert.NoError(t, err, "Range 1 processing should succeed")

				// Verify range was processed
				lastRange, err := repo.GetLastIndexedRange(ctx)
				assert.NoError(t, err, "Should be able to get last indexed range")
				assert.Equal(t, uint64(1), lastRange, "Range 1 should be processed")
			})

			t.Run("RangeCalculations", func(t *testing.T) {
				// Test range number calculation
				rangeNum := rangeProcessor.GetRangeNumber(uint64(tt.rangeSize + 1))
				assert.Equal(t, uint64(1), rangeNum, "Range number calculation should be correct")

				// Test range block numbers
				start, end := rangeProcessor.GetRangeBlockNumbers(1)
				assert.Equal(t, uint64(1), start, "Range start should be correct")
				assert.Equal(t, uint64(tt.rangeSize), end, "Range end should be correct")
			})
		})
	}
}

// TestRangeFileOperations tests range file creation, reading, and validation
func TestRangeFileOperations(t *testing.T) {
	t.Run("Range file creation and reading", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		// Create mock RPC client
		mockRPC := NewMockRPCClient()
		mockRPC.SetLatestBlock(200)

		// Set up realistic state diff responses
		stateDiffs := []rpc.TransactionResult{
			{
				TxHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				StateDiff: map[string]rpc.AccountDiff{
					"0x1111111111111111111111111111111111111111": {
						Balance: map[string]any{
							"from": "0x0",
							"to":   "0x1000000000000000000",
						},
					},
					"0x2222222222222222222222222222222222222222": {
						Code: map[string]any{
							"from": "0x",
							"to":   "0x608060405234801561001057600080fd5b50",
						},
						Storage: map[string]any{
							"0x0000000000000000000000000000000000000000000000000000000000000001": map[string]any{
								"from": "0x0000000000000000000000000000000000000000000000000000000000000000",
								"to":   "0x0000000000000000000000000000000000000000000000000000000000000001",
							},
						},
					},
				},
			},
		}
		mockRPC.SetStateDiffResponse(stateDiffs)

		// Create range processor
		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 100)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		ctx := context.Background()

		// Test range file creation
		t.Run("CreateRangeFile", func(t *testing.T) {
			// Download and create range file
			err := rangeProcessor.DownloadRange(ctx, 1)
			assert.NoError(t, err, "Range download should succeed")

			// Verify file exists
			assert.True(t, rangeProcessor.RangeExists(1), "Range file should exist")

			// Verify file path is correct
			expectedPath := filepath.Join(dataDir, "1_100.json.zst")
			actualPath := rangeProcessor.GetRangeFilePath(1)
			assert.Equal(t, expectedPath, actualPath, "Range file path should be correct")
		})

		t.Run("ReadRangeFile", func(t *testing.T) {
			// Read the range file
			rangeDiffs, err := rangeProcessor.ReadRange(1)
			assert.NoError(t, err, "Range reading should succeed")
			assert.NotEmpty(t, rangeDiffs, "Range data should not be empty")

			// Verify structure
			assert.Equal(t, 100, len(rangeDiffs), "Should have 100 blocks in range")

			// Verify first block
			firstBlock := rangeDiffs[0]
			assert.Equal(t, uint64(1), firstBlock.BlockNum, "First block should be block 1")
			assert.NotEmpty(t, firstBlock.Diffs, "First block should have diffs")
		})

		t.Run("EnsureRangeExists", func(t *testing.T) {
			// Test with existing range
			err := rangeProcessor.EnsureRangeExists(ctx, 1)
			assert.NoError(t, err, "EnsureRangeExists should succeed for existing range")

			// Test with non-existing range
			err = rangeProcessor.EnsureRangeExists(ctx, 2)
			assert.NoError(t, err, "EnsureRangeExists should create new range")
			assert.True(t, rangeProcessor.RangeExists(2), "New range should exist after EnsureRangeExists")
		})
	})

	t.Run("Invalid range file handling", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		mockRPC := NewMockRPCClient()
		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 100)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		// Create invalid range file
		invalidPath := filepath.Join(dataDir, "1_100.json.zst")
		err = os.WriteFile(invalidPath, []byte("invalid compressed data"), 0o644)
		require.NoError(t, err, "Failed to create invalid range file")

		// Try to read invalid range file
		_, err = rangeProcessor.ReadRange(1)
		assert.Error(t, err, "Reading invalid range file should fail")
		assert.Contains(t, err.Error(), "decompress", "Error should mention decompression")
	})
}

// TestRangeMetadataTracking tests range metadata tracking in the database
func TestRangeMetadataTracking(t *testing.T) {
	tests := []struct {
		name        string
		archiveMode bool
	}{
		{
			name:        "PostgreSQL Metadata Tracking",
			archiveMode: false,
		},
		{
			name:        "ClickHouse Metadata Tracking",
			archiveMode: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test environment
			dataDir, cleanupDir := createTestDataDir(t)
			defer cleanupDir()

			config := createTestConfig(tt.archiveMode, dataDir)
			repo, cleanupDB := createTestRepository(t, tt.archiveMode, config)
			defer cleanupDB()

			ctx := context.Background()

			// Test initial metadata state
			t.Run("InitialState", func(t *testing.T) {
				lastRange, err := repo.GetLastIndexedRange(ctx)
				assert.NoError(t, err, "Should be able to get initial last indexed range")
				assert.Equal(t, uint64(0), lastRange, "Initial last indexed range should be 0")
			})

			// Test metadata updates
			t.Run("MetadataUpdates", func(t *testing.T) {
				// Create appropriate state access
				var sa StateAccess
				if tt.archiveMode {
					sa = newStateAccessArchive()
				} else {
					sa = newStateAccessLatest()
				}

				// Simulate processing range 1
				err := sa.AddAccount("0x1111111111111111111111111111111111111111", 100, false)
				require.NoError(t, err, "Should be able to add account")

				err = sa.Commit(ctx, repo, 1)
				assert.NoError(t, err, "Should be able to commit range 1")

				// Verify metadata was updated
				lastRange, err := repo.GetLastIndexedRange(ctx)
				assert.NoError(t, err, "Should be able to get last indexed range")
				assert.Equal(t, uint64(1), lastRange, "Last indexed range should be updated to 1")

				// Process range 2
				sa.Reset()
				err = sa.AddAccount("0x2222222222222222222222222222222222222222", 200, true)
				require.NoError(t, err, "Should be able to add account")

				err = sa.Commit(ctx, repo, 2)
				assert.NoError(t, err, "Should be able to commit range 2")

				// Verify metadata was updated again
				lastRange, err = repo.GetLastIndexedRange(ctx)
				assert.NoError(t, err, "Should be able to get last indexed range")
				assert.Equal(t, uint64(2), lastRange, "Last indexed range should be updated to 2")
			})
		})
	}
}

// TestRangeErrorRecovery tests error recovery and retry logic
func TestRangeErrorRecovery(t *testing.T) {
	t.Run("RPC error recovery", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		// Create mock RPC client that fails first, then succeeds
		mockRPC := &FailingMockRPCClient{
			failCount: 3,
			mockRPC:   NewMockRPCClient(),
		}

		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 100)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		ctx := context.Background()

		// First attempts should fail
		err = rangeProcessor.DownloadRange(ctx, 1)
		assert.Error(t, err, "Should fail on first attempt")

		// Reset fail count and try again
		mockRPC.failCount = 0
		err = rangeProcessor.DownloadRange(ctx, 1)
		assert.NoError(t, err, "Should succeed after reset")
	})

	t.Run("Database transaction error handling", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(false, dataDir)
		repo, cleanupDB := createTestRepository(t, false, config)
		defer cleanupDB()

		// Close the database connection to simulate errors
		cleanupDB()

		// Create state access
		sa := newStateAccessLatest()
		err := sa.AddAccount("0x1111111111111111111111111111111111111111", 100, false)
		require.NoError(t, err, "Should be able to add account")

		ctx := context.Background()

		// Commit should fail due to closed database
		err = sa.Commit(ctx, repo, 1)
		assert.Error(t, err, "Should fail when database is closed")
	})

	t.Run("Context cancellation handling", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		mockRPC := NewMockRPCClient()
		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 100)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		// Create context with immediate cancellation
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Download should handle cancellation
		err = rangeProcessor.DownloadRange(ctx, 1)
		if err != nil {
			assert.Contains(t, err.Error(), "context", "Error should be context-related")
		}
	})
}

// TestLargeRangeProcessing tests processing of large ranges
func TestLargeRangeProcessing(t *testing.T) {
	t.Run("Large range with many blocks", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(false, dataDir)
		config.RangeSize = 1000 // Large range size

		repo, cleanupDB := createTestRepository(t, false, config)
		defer cleanupDB()

		// Create mock RPC client
		mockRPC := NewMockRPCClient()
		mockRPC.SetLatestBlock(1000)

		// Create range processor
		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 1000)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		// Create indexer
		indexer := NewIndexer(repo, rangeProcessor, mockRPC, config)
		ctx := context.Background()

		// Create state access
		sa := newStateAccessLatest()

		// Process large range
		start := time.Now()
		err = indexer.ProcessRange(ctx, 1, sa, true)
		duration := time.Since(start)

		assert.NoError(t, err, "Large range processing should succeed")
		assert.Less(t, duration, 30*time.Second, "Large range should process within 30 seconds")

		// Verify range was processed
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err, "Should be able to get last indexed range")
		assert.Equal(t, uint64(1), lastRange, "Large range should be processed")
	})
}

// TestConcurrentRangeProcessing tests concurrent range processing scenarios
func TestConcurrentRangeProcessing(t *testing.T) {
	t.Run("Concurrent range file creation", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		mockRPC := NewMockRPCClient()
		rangeProcessor, err := storage.NewRangeProcessor(dataDir, mockRPC, 100)
		require.NoError(t, err, "Failed to create range processor")
		defer rangeProcessor.Close()

		ctx := context.Background()

		// Create multiple goroutines trying to create the same range
		done := make(chan error, 3)
		for i := 0; i < 3; i++ {
			go func() {
				err := rangeProcessor.EnsureRangeExists(ctx, 1)
				done <- err
			}()
		}

		// Wait for all goroutines to complete
		var errors []error
		for i := 0; i < 3; i++ {
			err := <-done
			if err != nil {
				errors = append(errors, err)
			}
		}

		// At least one should succeed
		assert.LessOrEqual(t, len(errors), 2, "At most 2 out of 3 concurrent operations should fail")
		assert.True(t, rangeProcessor.RangeExists(1), "Range should exist after concurrent operations")
	})
}

// FailingMockRPCClient wraps MockRPCClient to simulate failures
type FailingMockRPCClient struct {
	failCount int
	mockRPC   *MockRPCClient
}

func (f *FailingMockRPCClient) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	if f.failCount > 0 {
		f.failCount--
		return nil, fmt.Errorf("simulated RPC failure")
	}
	return f.mockRPC.GetLatestBlockNumber(ctx)
}

func (f *FailingMockRPCClient) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	if f.failCount > 0 {
		f.failCount--
		return "", fmt.Errorf("simulated RPC failure")
	}
	return f.mockRPC.GetCode(ctx, address, blockNumber)
}

func (f *FailingMockRPCClient) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]rpc.TransactionResult, error) {
	if f.failCount > 0 {
		f.failCount--
		return nil, fmt.Errorf("simulated RPC failure")
	}
	return f.mockRPC.GetStateDiff(ctx, blockNumber)
}
