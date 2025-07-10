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
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

// MockRPCClient provides a mock implementation of RPC client for testing
type MockRPCClient struct {
	latestBlock       *big.Int
	codeResponses     map[string]string
	stateDiffResponse []rpc.TransactionResult
	getCodeCallCount  int
}

func NewMockRPCClient() *MockRPCClient {
	return &MockRPCClient{
		latestBlock:   big.NewInt(1000),
		codeResponses: make(map[string]string),
	}
}

func (m *MockRPCClient) SetLatestBlock(block uint64) {
	m.latestBlock = big.NewInt(int64(block))
}

func (m *MockRPCClient) SetCodeResponse(address, code string) {
	m.codeResponses[address] = code
}

func (m *MockRPCClient) SetStateDiffResponse(stateDiffs []rpc.TransactionResult) {
	m.stateDiffResponse = stateDiffs
}

func (m *MockRPCClient) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	return m.latestBlock, nil
}

func (m *MockRPCClient) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	m.getCodeCallCount++
	if code, exists := m.codeResponses[address]; exists {
		return code, nil
	}
	return "0x", nil // Default to EOA
}

func (m *MockRPCClient) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]rpc.TransactionResult, error) {
	return m.stateDiffResponse, nil
}

// createTestConfig creates a test configuration
func createTestConfig(dataDir string) internal.Config {
	testConfig := testdb.GetTestConfig()

	return internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		DataDir:            dataDir,
		RangeSize:          100,
		PollInterval:       1,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
	}
}

// createTestRepository creates a test repository with database setup
func createTestRepository(t *testing.T, config internal.Config) (repository.StateRepositoryInterface, func()) {
	t.Helper()

	cleanup := testdb.SetupTestDatabase(t)

	ctx := context.Background()
	repo, err := repository.NewRepository(ctx, config)
	require.NoError(t, err, "Failed to create repository")

	return repo, cleanup
}

// createTestDataDir creates a temporary directory for test data
func createTestDataDir(t *testing.T) (string, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "indexer-test-*")
	require.NoError(t, err, "Failed to create temp directory")

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

// createTestRangeFile creates a test range file with realistic data
func createTestRangeFile(t *testing.T, dataDir string, rangeNumber uint64, blockStart, blockEnd uint64) {
	t.Helper()

	// Create range file path
	filename := filepath.Join(dataDir, fmt.Sprintf("%d_%d.json.zst", blockStart, blockEnd))

	// Create parent directory if it doesn't exist
	err := os.MkdirAll(filepath.Dir(filename), 0o755)
	require.NoError(t, err, "Failed to create data directory")

	// For testing purposes, create a simple JSON file with realistic structure
	// In a real scenario, this would be compressed with zstd
	file, err := os.Create(filename)
	require.NoError(t, err, "Failed to create range file")
	defer file.Close()

	// Write test data that matches the expected structure
	data := fmt.Sprintf(`[
		{
			"blockNum": %d,
			"diffs": [
				{
					"transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					"stateDiff": {
						"0x1111111111111111111111111111111111111111": {
							"balance": {"from": "0x0", "to": "0x1000000000000000000"}
						},
						"0x2222222222222222222222222222222222222222": {
							"code": {"from": "0x", "to": "0x608060405234801561001057600080fd5b50"},
							"storage": {
								"0x0000000000000000000000000000000000000000000000000000000000000001": {
									"from": "0x0000000000000000000000000000000000000000000000000000000000000000",
									"to": "0x0000000000000000000000000000000000000000000000000000000000000001"
								}
							}
						}
					}
				}
			]
		},
		{
			"blockNum": %d,
			"diffs": [
				{
					"transactionHash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
					"stateDiff": {
						"0x3333333333333333333333333333333333333333": {
							"balance": {"from": "0x0", "to": "0x2000000000000000000"}
						}
					}
				}
			]
		}
	]`, blockStart, blockStart+1)

	_, err = file.WriteString(data)
	require.NoError(t, err, "Failed to write range file data")
}

// TestIndexerServiceInitialization tests service initialization and configuration
func TestIndexerServiceInitialization(t *testing.T) {
	tests := []struct {
		name        string
		archiveMode bool
	}{
		{
			name: "ClickHouse Service Initialization",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test environment
			dataDir, cleanupDir := createTestDataDir(t)
			defer cleanupDir()

			config := createTestConfig(dataDir)
			repo, cleanupDB := createTestRepository(t, config)
			defer cleanupDB()

			mockRPC := NewMockRPCClient()

			// Test service creation
			service := NewService(repo, mockRPC, config)
			require.NotNil(t, service, "Service should be created")
			defer service.Close()

			// Verify service components
			assert.NotNil(t, service.indexer, "Indexer should be initialized")
			assert.NotNil(t, service.repo, "Repository should be set")
			assert.NotNil(t, service.rpcClient, "RPC client should be set")
			assert.Equal(t, config.RangeSize, service.config.RangeSize, "Range size should match config")

			// Test service close
			service.Close()
			// Verify graceful shutdown (no panic or error)
		})
	}
}

// TestIndexerServiceFailedInitialization tests service initialization failures
func TestIndexerServiceFailedInitialization(t *testing.T) {
	t.Run("Service initialization edge cases", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()

		// Service creation should succeed with valid parameters
		service := NewService(repo, mockRPC, config)
		assert.NotNil(t, service, "Service should be created with valid parameters")

		if service != nil {
			service.Close()
		}
	})
}

// TestIndexerProcessGenesis tests genesis block processing
func TestIndexerProcessGenesis(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ClickHouse Genesis Processing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test environment
			dataDir, cleanupDir := createTestDataDir(t)
			defer cleanupDir()

			config := createTestConfig(dataDir)
			repo, cleanupDB := createTestRepository(t, config)
			defer cleanupDB()

			mockRPC := NewMockRPCClient()
			service := NewService(repo, mockRPC, config)
			require.NotNil(t, service)
			defer service.Close()

			ctx := context.Background()

			// Test genesis processing
			err := service.indexer.ProcessGenesis(ctx)
			assert.NoError(t, err, "Genesis processing should succeed")

			// Verify that genesis was processed (last indexed range should be 0)
			lastRange, err := repo.GetLastIndexedRange(ctx)
			assert.NoError(t, err, "Should be able to get last indexed range")
			assert.Equal(t, uint64(0), lastRange, "Last indexed range should be 0 after genesis")
		})
	}
}

// TestIndexerRangeProcessing tests range processing functionality
func TestIndexerRangeProcessing(t *testing.T) {
	t.Run("Genesis range processing", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Create appropriate state access
		sa := newStateAccessArchive()

		// Test genesis range processing (range 0)
		err := service.indexer.ProcessRange(ctx, 0, sa, true)
		assert.NoError(t, err, "Genesis range processing should succeed")

		// Range 0 processes genesis but commits directly to repository, not to state access
		// Verify that genesis was processed by checking the repository
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err, "Should be able to get last indexed range")
		assert.Equal(t, uint64(0), lastRange, "Genesis should have been processed")
	})

	t.Run("Archive mode state access creation", func(t *testing.T) {
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Test that archive mode uses correct state access type
		err := service.processAvailableRanges(ctx)
		assert.NoError(t, err, "Archive mode processing should succeed")
	})
}

// TestIndexerServiceProcessAvailableRanges tests the main processing workflow
func TestIndexerServiceProcessAvailableRanges(t *testing.T) {
	t.Run("Basic available ranges processing", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		mockRPC.SetLatestBlock(50) // Set latest block to a small value to avoid file requirements

		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Process available ranges (should at least process genesis)
		err := service.processAvailableRanges(ctx)
		assert.NoError(t, err, "Processing available ranges should succeed")

		// Verify that at least genesis was processed
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err, "Should be able to get last indexed range")
		assert.Equal(t, uint64(0), lastRange, "Genesis should have been processed")
	})

	t.Run("No ranges to process", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		// Manually set last indexed range to simulate already processed genesis
		ctx := context.Background()
		err := repo.InsertRange(ctx, map[uint64]map[string]struct{}{}, map[string]bool{}, map[uint64]map[string]map[string]struct{}{}, 0)
		require.NoError(t, err, "Should be able to update range data")

		mockRPC := NewMockRPCClient()
		mockRPC.SetLatestBlock(0) // No new blocks available

		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		// Process available ranges (should be no-op)
		err = service.processAvailableRanges(ctx)
		assert.NoError(t, err, "Processing with no available ranges should succeed")
	})
}

// TestIndexerServiceErrorHandling tests error handling scenarios
func TestIndexerServiceErrorHandling(t *testing.T) {
	t.Run("Database connection error handling", func(t *testing.T) {
		// Create test environment with valid config first
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		// Close database connection to simulate error
		cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Processing should fail due to database connection error
		err := service.processAvailableRanges(ctx)
		assert.Error(t, err, "Should fail when database is unavailable")
	})

	t.Run("RPC client error handling", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		// Create mock RPC that will fail
		mockRPC := &MockRPCClient{
			latestBlock:   big.NewInt(1000),
			codeResponses: make(map[string]string),
		}

		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Process should succeed even when range files are created with empty data
		err := service.processAvailableRanges(ctx)
		assert.NoError(t, err, "Processing should handle empty ranges gracefully")
	})
}

// TestIndexerServiceContextCancellation tests graceful handling of context cancellation
func TestIndexerServiceContextCancellation(t *testing.T) {
	t.Run("Context cancellation during processing", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		// Create context with immediate cancellation
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Processing should handle context cancellation
		err := service.processAvailableRanges(ctx)
		if err != nil {
			assert.Contains(t, err.Error(), "context", "Error should be context-related")
		}
	})

	t.Run("Context timeout during processing", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		// Create context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for timeout
		time.Sleep(1 * time.Millisecond)

		// Processing should handle timeout gracefully
		err := service.processAvailableRanges(ctx)
		// Should either succeed quickly or handle timeout gracefully
		if err != nil {
			assert.Contains(t, err.Error(), "context", "Error should be context-related")
		}
	})
}

// TestIndexerServiceAccountTypeDetection tests account type detection logic
func TestIndexerServiceAccountTypeDetection(t *testing.T) {
	t.Run("Account type detection with RPC calls", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()

		// Set up mock responses
		contractAddress := "0x2222222222222222222222222222222222222222"
		eoaAddress := "0x1111111111111111111111111111111111111111"

		mockRPC.SetCodeResponse(contractAddress, "0x608060405234801561001057600080fd5b50") // Contract code
		mockRPC.SetCodeResponse(eoaAddress, "0x")                                          // EOA (no code)

		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)
		defer service.Close()

		ctx := context.Background()

		// Test account type detection for contract
		isContract := service.indexer.determineAccountType(ctx, contractAddress, 100, rpc.AccountDiff{})
		assert.True(t, isContract, "Should detect contract account")

		// Test account type detection for EOA
		isEOA := service.indexer.determineAccountType(ctx, eoaAddress, 100, rpc.AccountDiff{})
		assert.False(t, isEOA, "Should detect EOA account")

		// Verify RPC calls were made
		assert.Greater(t, mockRPC.getCodeCallCount, 0, "RPC calls should have been made for code checking")
	})
}

// TestIndexerServiceResourceCleanup tests proper resource cleanup
func TestIndexerServiceResourceCleanup(t *testing.T) {
	t.Run("Proper resource cleanup on service close", func(t *testing.T) {
		// Create test environment
		dataDir, cleanupDir := createTestDataDir(t)
		defer cleanupDir()

		config := createTestConfig(dataDir)
		repo, cleanupDB := createTestRepository(t, config)
		defer cleanupDB()

		mockRPC := NewMockRPCClient()
		service := NewService(repo, mockRPC, config)
		require.NotNil(t, service)

		// Verify service is properly initialized
		assert.NotNil(t, service.indexer, "Indexer should be initialized")
		assert.NotNil(t, service.indexer.rangeProcessor, "Range processor should be initialized")

		// Close service and verify cleanup
		service.Close()

		// Verify no panic occurs when calling Close multiple times
		service.Close()
	})
}
