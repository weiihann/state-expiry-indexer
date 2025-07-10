package storage

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

// MockRPCClient implements rpc.ClientInterface for testing
type MockRPCClient struct {
	mu                   sync.RWMutex
	mockResponses        map[string][]rpc.TransactionResult
	errorResponses       map[string]error
	callCount            map[string]int
	latestBlockNumber    *big.Int
	simulateSlowResponse bool
	simulateTimeout      bool
	simulateNetworkError bool
	getStateDiffDelay    time.Duration
}

func NewMockRPCClient() *MockRPCClient {
	return &MockRPCClient{
		mockResponses:     make(map[string][]rpc.TransactionResult),
		errorResponses:    make(map[string]error),
		callCount:         make(map[string]int),
		latestBlockNumber: big.NewInt(100000),
	}
}

func (m *MockRPCClient) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.simulateNetworkError {
		return nil, fmt.Errorf("network error: connection refused")
	}

	m.mu.Lock()
	m.callCount["GetLatestBlockNumber"]++
	m.mu.Unlock()
	return new(big.Int).Set(m.latestBlockNumber), nil
}

func (m *MockRPCClient) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.simulateNetworkError {
		return "", fmt.Errorf("network error: connection refused")
	}

	m.mu.Lock()
	m.callCount["GetCode"]++
	m.mu.Unlock()
	return "0x", nil // Return empty code for simplicity
}

func (m *MockRPCClient) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]rpc.TransactionResult, error) {
	// Read configuration values under lock
	m.mu.RLock()
	simulateTimeout := m.simulateTimeout
	simulateNetworkError := m.simulateNetworkError
	delay := m.getStateDiffDelay
	blockKey := blockNumber.String()

	// Check for specific error responses
	err, hasError := m.errorResponses[blockKey]

	// Check for specific mock responses
	response, hasResponse := m.mockResponses[blockKey]
	m.mu.RUnlock()

	if simulateTimeout {
		// Simulate timeout by waiting longer than context allows
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			// Should not reach here if context has proper timeout
		}
	}

	if simulateNetworkError {
		return nil, fmt.Errorf("network error: connection refused")
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if hasError {
		return nil, err
	}

	// Increment call count under separate lock
	m.mu.Lock()
	m.callCount["GetStateDiff"]++
	m.mu.Unlock()

	if hasResponse {
		return response, nil
	}

	// Default response for any block
	return []rpc.TransactionResult{
		{
			TxHash: fmt.Sprintf("0x%064d", blockNumber.Uint64()),
			StateDiff: map[string]rpc.AccountDiff{
				fmt.Sprintf("0x%040d", blockNumber.Uint64()): {
					Balance: map[string]any{
						"from": "0x0",
						"to":   fmt.Sprintf("0x%x", blockNumber.Uint64()*1000),
					},
					Nonce: map[string]any{
						"from": "0x0",
						"to":   fmt.Sprintf("0x%x", blockNumber.Uint64()),
					},
				},
			},
		},
	}, nil
}

// Helper methods for configuring mock behavior
func (m *MockRPCClient) SetMockResponse(blockNumber *big.Int, response []rpc.TransactionResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mockResponses[blockNumber.String()] = response
}

func (m *MockRPCClient) SetErrorResponse(blockNumber *big.Int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorResponses[blockNumber.String()] = err
}

func (m *MockRPCClient) SetLatestBlockNumber(blockNumber *big.Int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestBlockNumber = new(big.Int).Set(blockNumber)
}

func (m *MockRPCClient) GetCallCount(method string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callCount[method]
}

func (m *MockRPCClient) SimulateNetworkError(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulateNetworkError = enabled
}

func (m *MockRPCClient) SimulateTimeout(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulateTimeout = enabled
}

func (m *MockRPCClient) SetGetStateDiffDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getStateDiffDelay = delay
}

func setupRangeProcessorTest(t *testing.T) (*RangeProcessor, *MockRPCClient, string, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "rangeprocessor_test")
	require.NoError(t, err)

	// Create mock RPC client
	mockClient := NewMockRPCClient()

	// Create range processor
	rp, err := NewRangeProcessor(tempDir, mockClient, 100) // 100 blocks per range
	require.NoError(t, err)

	cleanup := func() {
		rp.Close()
		os.RemoveAll(tempDir)
	}

	return rp, mockClient, tempDir, cleanup
}

func TestRangeProcessorInitialization(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		assert.NotNil(t, rp)
		assert.NotNil(t, rp.encoder)
		assert.NotNil(t, rp.decoder)
		assert.NotNil(t, rp.rpcClient)
		assert.Equal(t, 100, rp.rangeSize)
	})

	t.Run("initialization with different range sizes", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "rangeprocessor_test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		mockClient := NewMockRPCClient()

		// Test different range sizes
		rangeSizes := []int{50, 100, 500, 1000}
		for _, size := range rangeSizes {
			rp, err := NewRangeProcessor(tempDir, mockClient, size)
			require.NoError(t, err)
			assert.Equal(t, size, rp.rangeSize)
			rp.Close()
		}
	})

	t.Run("initialization with invalid data directory", func(t *testing.T) {
		mockClient := NewMockRPCClient()

		// Test with non-existent directory (should still work as directory will be created)
		rp, err := NewRangeProcessor("/non/existent/directory", mockClient, 100)
		require.NoError(t, err)
		assert.NotNil(t, rp)
		rp.Close()
	})

	t.Run("proper resource cleanup", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)

		// Verify resources are allocated
		assert.NotNil(t, rp.encoder)
		assert.NotNil(t, rp.decoder)

		// Close and verify cleanup
		cleanup()

		// Resources should be cleaned up (encoder/decoder closed)
		// Note: We can't directly test this as Close() doesn't nil the pointers
		// but the underlying resources are closed
	})
}

func TestRangeProcessorDownloadRange(t *testing.T) {
	t.Run("successful range download", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Download range
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify file was created
		filePath := rp.GetRangeFilePath(rangeNumber)
		assert.True(t, rp.RangeExists(rangeNumber))

		// Verify file exists on disk
		_, err = os.Stat(filePath)
		assert.NoError(t, err)

		// Verify RPC calls were made
		assert.Equal(t, 100, mockClient.GetCallCount("GetStateDiff")) // 100 blocks in range
	})

	t.Run("download range with existing file", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// First download
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Reset call count
		mockClient.mu.Lock()
		mockClient.callCount = make(map[string]int)
		mockClient.mu.Unlock()

		// Second download - should skip
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Should not make any RPC calls
		assert.Equal(t, 0, mockClient.GetCallCount("GetStateDiff"))
	})

	t.Run("download range with context cancellation", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		// Create context with immediate cancellation
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		rangeNumber := uint64(1)

		// Download should fail with context error
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("download range with RPC errors", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Set error for block 50 (middle of range)
		mockClient.SetErrorResponse(big.NewInt(50), fmt.Errorf("block not found"))

		// Download should fail
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to download block 50")
		assert.Contains(t, err.Error(), "block not found")

		// File should not exist
		assert.False(t, rp.RangeExists(rangeNumber))
	})

	t.Run("download range with network errors", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Simulate network error
		mockClient.SimulateNetworkError(true)

		// Download should fail
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "network error")
	})

	t.Run("cannot download genesis range", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(0) // Genesis

		// Should return error for genesis
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot download genesis range")
	})
}

func TestRangeProcessorReadRange(t *testing.T) {
	t.Run("successful range reading", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// First download the range
		err := rp.DownloadRange(ctx, rangeNumber)
		require.NoError(t, err)

		// Read the range
		rangeDiffs, err := rp.ReadRange(rangeNumber)
		assert.NoError(t, err)
		assert.Len(t, rangeDiffs, 100) // 100 blocks in range

		// Verify block numbers are correct
		for i, diff := range rangeDiffs {
			expectedBlock := uint64(i + 1) // Range 1 = blocks 1-100
			assert.Equal(t, expectedBlock, diff.BlockNum)
			assert.Len(t, diff.Diffs, 1) // Each block has 1 transaction
		}
	})

	t.Run("read non-existent range", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		rangeNumber := uint64(999) // Non-existent range

		// Should return error
		_, err := rp.ReadRange(rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read range file")
	})

	t.Run("cannot read genesis range", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		rangeNumber := uint64(0) // Genesis

		// Should return error for genesis
		_, err := rp.ReadRange(rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot read genesis as range")
	})

	t.Run("read corrupted range file", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		rangeNumber := uint64(1)
		filePath := rp.GetRangeFilePath(rangeNumber)

		// Create corrupted file
		err := os.WriteFile(filePath, []byte("corrupted data"), 0o644)
		require.NoError(t, err)

		// Should return error
		_, err = rp.ReadRange(rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decompress range file")
	})
}

func TestRangeProcessorEnsureRangeExists(t *testing.T) {
	t.Run("ensure non-existent range", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Initially should not exist
		assert.False(t, rp.RangeExists(rangeNumber))

		// Ensure range exists
		err := rp.EnsureRangeExists(ctx, rangeNumber)
		assert.NoError(t, err)

		// Should exist now
		assert.True(t, rp.RangeExists(rangeNumber))
	})

	t.Run("ensure existing range", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// First download
		err := rp.DownloadRange(ctx, rangeNumber)
		require.NoError(t, err)

		// Reset call count
		mockClient.mu.Lock()
		mockClient.callCount = make(map[string]int)
		mockClient.mu.Unlock()

		// Ensure range exists - should not download again
		err = rp.EnsureRangeExists(ctx, rangeNumber)
		assert.NoError(t, err)

		// Should not make any RPC calls
		assert.Equal(t, 0, mockClient.GetCallCount("GetStateDiff"))
	})

	t.Run("ensure genesis range", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(0) // Genesis

		// Should always succeed for genesis
		err := rp.EnsureRangeExists(ctx, rangeNumber)
		assert.NoError(t, err)
	})
}

func TestRangeProcessorFileSystemIntegration(t *testing.T) {
	t.Run("multiple ranges in same directory", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		ranges := []uint64{1, 2, 3, 5, 10}

		// Download multiple ranges
		for _, rangeNum := range ranges {
			err := rp.DownloadRange(ctx, rangeNum)
			assert.NoError(t, err)
		}

		// Verify all ranges exist
		for _, rangeNum := range ranges {
			assert.True(t, rp.RangeExists(rangeNum))
		}

		// Verify file names are correct
		for _, rangeNum := range ranges {
			filePath := rp.GetRangeFilePath(rangeNum)
			_, err := os.Stat(filePath)
			assert.NoError(t, err)
		}
	})

	t.Run("range file permissions", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Download range
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Check file permissions
		filePath := rp.GetRangeFilePath(rangeNumber)
		info, err := os.Stat(filePath)
		assert.NoError(t, err)
		assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	})

	t.Run("directory creation", func(t *testing.T) {
		// Create nested directory path
		tempDir, err := os.MkdirTemp("", "rangeprocessor_test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		nestedDir := filepath.Join(tempDir, "nested", "subdir")
		mockClient := NewMockRPCClient()

		// Create the nested directory first (as the processor doesn't auto-create directories)
		err = os.MkdirAll(nestedDir, 0o755)
		require.NoError(t, err)

		// Create range processor with nested directory
		rp, err := NewRangeProcessor(nestedDir, mockClient, 100)
		require.NoError(t, err)
		defer rp.Close()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Download range
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify directory exists and contains the file
		_, err = os.Stat(nestedDir)
		assert.NoError(t, err)

		// Verify the range file was created in the nested directory
		filePath := rp.GetRangeFilePath(rangeNumber)
		_, err = os.Stat(filePath)
		assert.NoError(t, err)
	})
}

func TestRangeProcessorErrorHandling(t *testing.T) {
	t.Run("handle disk full scenario", func(t *testing.T) {
		// Note: This is a simulation - we can't actually fill the disk
		rp, _, tempDir, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Make directory read-only to simulate write error
		err := os.Chmod(tempDir, 0o444)
		if err != nil {
			t.Skip("Cannot change directory permissions on this system")
		}

		// Restore permissions in cleanup
		defer func() {
			os.Chmod(tempDir, 0o755)
		}()

		// Download should fail with permission error
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write range file")
	})

	t.Run("handle JSON marshaling error", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Create mock response with data that can't be marshaled
		// Note: In practice, this is hard to trigger with the current structures
		// but we can simulate it by creating a response that causes issues
		mockClient.SetMockResponse(big.NewInt(1), []rpc.TransactionResult{
			{
				TxHash: "valid_hash",
				StateDiff: map[string]rpc.AccountDiff{
					"0x1234567890123456789012345678901234567890": {
						Balance: make(chan int), // Channels can't be marshaled
					},
				},
			},
		})

		// Download should fail with JSON error
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal range data")
	})

	t.Run("handle invalid file write", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Create a file with the same name as the range file to cause write error
		filePath := rp.GetRangeFilePath(rangeNumber)
		err := os.WriteFile(filePath, []byte("test"), 0o444) // Read-only file
		require.NoError(t, err)

		// Download should succeed despite file existing (as it checks existence first)
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err) // Range exists, so no download attempted
	})
}

func TestRangeProcessorConcurrency(t *testing.T) {
	t.Run("concurrent range downloads", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		numRanges := 5
		ranges := make([]uint64, numRanges)
		for i := 0; i < numRanges; i++ {
			ranges[i] = uint64(i + 1)
		}

		// Download ranges concurrently
		var wg sync.WaitGroup
		errors := make(chan error, numRanges)

		for _, rangeNum := range ranges {
			wg.Add(1)
			go func(rn uint64) {
				defer wg.Done()
				err := rp.DownloadRange(ctx, rn)
				if err != nil {
					errors <- err
				}
			}(rangeNum)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			assert.NoError(t, err)
		}

		// Verify all ranges were downloaded
		for _, rangeNum := range ranges {
			assert.True(t, rp.RangeExists(rangeNum))
		}
	})

	t.Run("concurrent range reads", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// First download the range
		err := rp.DownloadRange(ctx, rangeNumber)
		require.NoError(t, err)

		// Read range concurrently
		numReads := 10
		var wg sync.WaitGroup
		errors := make(chan error, numReads)
		results := make(chan []RangeDiffs, numReads)

		for i := 0; i < numReads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				diffs, err := rp.ReadRange(rangeNumber)
				if err != nil {
					errors <- err
					return
				}
				results <- diffs
			}()
		}

		wg.Wait()
		close(errors)
		close(results)

		// Check for errors
		for err := range errors {
			assert.NoError(t, err)
		}

		// Verify all reads returned same data
		var firstResult []RangeDiffs
		resultCount := 0
		for result := range results {
			if resultCount == 0 {
				firstResult = result
			} else {
				assert.Equal(t, len(firstResult), len(result))
				// Compare first and last blocks
				assert.Equal(t, firstResult[0].BlockNum, result[0].BlockNum)
				assert.Equal(t, firstResult[len(firstResult)-1].BlockNum, result[len(result)-1].BlockNum)
			}
			resultCount++
		}

		assert.Equal(t, numReads, resultCount)
	})
}

func TestRangeProcessorProgressTracking(t *testing.T) {
	t.Run("track download progress", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()

		// Add delay to simulate slow RPC calls
		mockClient.SetGetStateDiffDelay(10 * time.Millisecond)

		rangeNumber := uint64(1)

		// Track download progress
		start := time.Now()
		err := rp.DownloadRange(ctx, rangeNumber)
		duration := time.Since(start)

		assert.NoError(t, err)

		// Should take at least 1 second (100 blocks * 10ms)
		assert.Greater(t, duration, time.Second)

		// Verify all blocks were downloaded
		assert.Equal(t, 100, mockClient.GetCallCount("GetStateDiff"))
	})

	t.Run("track file discovery", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		ranges := []uint64{1, 3, 5, 7, 9} // Non-contiguous ranges

		// Download multiple ranges
		for _, rangeNum := range ranges {
			err := rp.DownloadRange(ctx, rangeNum)
			assert.NoError(t, err)
		}

		// Verify file discovery by checking existence
		for _, rangeNum := range ranges {
			assert.True(t, rp.RangeExists(rangeNum))
		}

		// Verify missing ranges don't exist
		missingRanges := []uint64{2, 4, 6, 8, 10}
		for _, rangeNum := range missingRanges {
			assert.False(t, rp.RangeExists(rangeNum))
		}
	})
}

func TestRangeProcessorLargeDatasets(t *testing.T) {
	t.Run("handle large range size", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "rangeprocessor_test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		mockClient := NewMockRPCClient()

		// Create processor with large range size
		rp, err := NewRangeProcessor(tempDir, mockClient, 1000) // 1000 blocks per range
		require.NoError(t, err)
		defer rp.Close()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Download large range
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify correct number of RPC calls
		assert.Equal(t, 1000, mockClient.GetCallCount("GetStateDiff"))

		// Verify range exists
		assert.True(t, rp.RangeExists(rangeNumber))
	})

	t.Run("handle complex state diffs", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Create complex state diff with multiple accounts and storage changes
		complexStateDiff := []rpc.TransactionResult{
			{
				TxHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				StateDiff: map[string]rpc.AccountDiff{
					"0x1111111111111111111111111111111111111111": {
						Balance: map[string]any{
							"from": "0x1bc16d674ec80000",
							"to":   "0x1b1ae4d6e2ef5000",
						},
						Nonce: map[string]any{
							"from": "0x5",
							"to":   "0x6",
						},
						Storage: map[string]any{
							"0x0000000000000000000000000000000000000000000000000000000000000001": map[string]any{
								"from": "0x0",
								"to":   "0x123456789abcdef",
							},
							"0x0000000000000000000000000000000000000000000000000000000000000002": map[string]any{
								"from": "0xfedcba9876543210",
								"to":   "0x0",
							},
						},
					},
					"0x2222222222222222222222222222222222222222": {
						Code: map[string]any{
							"from": "0x",
							"to":   "0x6080604052348015600f57600080fd5b50",
						},
						Storage: map[string]any{
							"0x0000000000000000000000000000000000000000000000000000000000000000": map[string]any{
								"from": nil,
								"to":   "0x1234567890abcdef",
							},
						},
					},
				},
			},
		}

		// Set complex response for first block
		mockClient.SetMockResponse(big.NewInt(1), complexStateDiff)

		// Download range
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Read and verify complex data
		rangeDiffs, err := rp.ReadRange(rangeNumber)
		assert.NoError(t, err)
		assert.Len(t, rangeDiffs, 100)

		// Verify first block has complex data
		firstBlock := rangeDiffs[0]
		assert.Equal(t, uint64(1), firstBlock.BlockNum)
		assert.Len(t, firstBlock.Diffs, 1)
		assert.Equal(t, complexStateDiff[0].TxHash, firstBlock.Diffs[0].TxHash)
		assert.Len(t, firstBlock.Diffs[0].StateDiff, 2)
	})
}

func TestRangeProcessorIntegrationWithRealData(t *testing.T) {
	t.Run("process realistic block data", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Create realistic state diff data based on example.json
		realisticStateDiff := []rpc.TransactionResult{
			{
				TxHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				StateDiff: map[string]rpc.AccountDiff{
					"0x1234567890abcdef1234567890abcdef12345678": {
						Balance: map[string]any{
							"*": map[string]any{
								"from": "0x1bc16d674ec80000",
								"to":   "0x1b1ae4d6e2ef5000",
							},
						},
						Nonce: map[string]any{
							"*": map[string]any{
								"from": "0x5",
								"to":   "0x6",
							},
						},
						Storage: map[string]any{
							"0x0000000000000000000000000000000000000000000000000000000000000001": map[string]any{
								"*": map[string]any{
									"from": "0x0",
									"to":   "0x123456789abcdef",
								},
							},
						},
					},
				},
			},
		}

		// Set realistic response for all blocks
		for i := 1; i <= 100; i++ {
			mockClient.SetMockResponse(big.NewInt(int64(i)), realisticStateDiff)
		}

		// Download range
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Read and verify realistic data
		rangeDiffs, err := rp.ReadRange(rangeNumber)
		assert.NoError(t, err)
		assert.Len(t, rangeDiffs, 100)

		// Verify data structure matches expected format
		for _, diff := range rangeDiffs {
			assert.Len(t, diff.Diffs, 1)
			assert.Equal(t, realisticStateDiff[0].TxHash, diff.Diffs[0].TxHash)

			// Verify nested structure
			stateDiff := diff.Diffs[0].StateDiff
			assert.Len(t, stateDiff, 1)

			for addr, accountDiff := range stateDiff {
				assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", addr)
				assert.NotNil(t, accountDiff.Balance)
				assert.NotNil(t, accountDiff.Nonce)
				assert.NotNil(t, accountDiff.Storage)
			}
		}
	})
}

func TestRangeProcessorEmptyFileHandling(t *testing.T) {
	t.Run("treat empty range file as unavailable", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(1)

		// Create empty range file
		rangeFilePath := rp.GetRangeFilePath(rangeNumber)
		require.NoError(t, os.MkdirAll(filepath.Dir(rangeFilePath), 0o755))

		// Create empty file
		emptyFile, err := os.Create(rangeFilePath)
		require.NoError(t, err)
		require.NoError(t, emptyFile.Close())

		// Verify file exists but is empty
		fileInfo, err := os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), fileInfo.Size())

		// RangeExists should return false for empty file
		assert.False(t, rp.RangeExists(rangeNumber))

		// EnsureRangeExists should trigger download for empty file
		err = rp.EnsureRangeExists(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify that RPC calls were made (indicating download happened)
		assert.Equal(t, 100, mockClient.GetCallCount("GetStateDiff"))

		// Verify file now exists and is not empty
		fileInfo, err = os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))

		// RangeExists should now return true
		assert.True(t, rp.RangeExists(rangeNumber))
	})

	t.Run("handle empty file in DownloadRange", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(2)

		// Create empty range file
		rangeFilePath := rp.GetRangeFilePath(rangeNumber)
		require.NoError(t, os.MkdirAll(filepath.Dir(rangeFilePath), 0o755))

		// Create empty file
		emptyFile, err := os.Create(rangeFilePath)
		require.NoError(t, err)
		require.NoError(t, emptyFile.Close())

		// Verify file exists but is empty
		fileInfo, err := os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), fileInfo.Size())

		// Reset call count
		mockClient.callCount = make(map[string]int)

		// DownloadRange should proceed despite empty file existing
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify that RPC calls were made (indicating download happened)
		assert.Equal(t, 100, mockClient.GetCallCount("GetStateDiff"))

		// Verify file now exists and is not empty
		fileInfo, err = os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))

		// Verify we can read the range data
		rangeDiffs, err := rp.ReadRange(rangeNumber)
		assert.NoError(t, err)
		assert.Len(t, rangeDiffs, 100)
	})

	t.Run("normal file handling unchanged", func(t *testing.T) {
		rp, mockClient, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		ctx := context.Background()
		rangeNumber := uint64(3)

		// First download should work normally
		err := rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify file exists and is not empty
		rangeFilePath := rp.GetRangeFilePath(rangeNumber)
		fileInfo, err := os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))

		// RangeExists should return true
		assert.True(t, rp.RangeExists(rangeNumber))

		// Reset call count
		mockClient.callCount = make(map[string]int)

		// Second download should be skipped (file exists and is not empty)
		err = rp.DownloadRange(ctx, rangeNumber)
		assert.NoError(t, err)

		// Verify no RPC calls were made (download was skipped)
		assert.Equal(t, 0, mockClient.GetCallCount("GetStateDiff"))

		// File should still exist and be non-empty
		fileInfo, err = os.Stat(rangeFilePath)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))
	})

	t.Run("genesis range always exists", func(t *testing.T) {
		rp, _, _, cleanup := setupRangeProcessorTest(t)
		defer cleanup()

		// Genesis range should always be considered to exist
		assert.True(t, rp.RangeExists(0))

		// Even if we create an empty file at genesis location, it should still exist
		// (though genesis doesn't have a file path anyway)
		assert.True(t, rp.RangeExists(0))
	})
}
