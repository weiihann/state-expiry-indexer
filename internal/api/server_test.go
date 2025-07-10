package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

// TestAPIServer tests the API server endpoints with database integration
func TestAPIServer(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ClickHouse API Server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			repo, cleanupDB := setupTestRepository(t)
			defer cleanupDB()

			// Create mock RPC client
			mockRPC := createMockRPCClient()

			// Create API server
			rangeSize := uint64(100)
			server := NewTestServer(repo, mockRPC, rangeSize)

			// Setup test data
			setupTestData(t, repo)

			// Test all endpoints
			t.Run("HealthCheck", func(t *testing.T) {
				testHealthCheckEndpoint(t, server)
			})

			t.Run("Analytics", func(t *testing.T) {
				testAnalyticsEndpoint(t, server)
			})

			t.Run("Sync", func(t *testing.T) {
				testSyncEndpoint(t, server)
			})
		})
	}
}

// TestAPIEndpointErrorHandling tests error handling for all endpoints
func TestAPIEndpointErrorHandling(t *testing.T) {
	repo, cleanupDB := setupTestRepository(t)
	defer cleanupDB()

	mockRPC := createMockRPCClient()
	rangeSize := uint64(100)
	server := NewTestServer(repo, mockRPC, rangeSize)

	// Create a test router for the server
	router := createTestRouter(server)

	t.Run("Analytics with invalid expiry_block", func(t *testing.T) {
		// Test with invalid expiry_block parameter
		req, err := http.NewRequest("GET", "/api/v1/stats/analytics?expiry_block=invalid", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Invalid 'expiry_block' query parameter")
	})

	t.Run("Analytics with missing expiry_block", func(t *testing.T) {
		// Test with missing expiry_block parameter
		req, err := http.NewRequest("GET", "/api/v1/stats/analytics", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "expiry_block")
	})

	t.Run("Invalid endpoint", func(t *testing.T) {
		// Test with non-existent endpoint
		req, err := http.NewRequest("GET", "/api/v1/invalid", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

// TestAPIServerWithDatabaseFailure tests API behavior when database is unavailable
func TestAPIServerWithDatabaseFailure(t *testing.T) {
	repo, cleanupDB := setupTestRepository(t)
	cleanupDB() // Close database connection to simulate failure

	mockRPC := createMockRPCClient()
	rangeSize := uint64(100)
	server := NewTestServer(repo, mockRPC, rangeSize)

	// Create a test router for the server
	router := createTestRouter(server)

	t.Run("Analytics with database failure", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/api/v1/stats/analytics?expiry_block=100", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Could not get analytics data")
	})

	t.Run("Sync with database failure", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/api/v1/sync", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Could not get sync status")
	})
}

// TestAPIServerWithRPCFailure tests API behavior when RPC client fails
func TestAPIServerWithRPCFailure(t *testing.T) {
	repo, cleanupDB := setupTestRepository(t)
	defer cleanupDB()

	// Create failing RPC client
	mockRPC := &FailingRPCWrapper{}
	rangeSize := uint64(100)
	server := NewTestServer(repo, mockRPC, rangeSize)

	// Create a test router for the server
	router := createTestRouter(server)

	t.Run("Sync with RPC failure", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/api/v1/sync", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Could not get latest block number")
	})
}

// TestAPIServerConcurrency tests concurrent requests to API endpoints
func TestAPIServerConcurrency(t *testing.T) {
	repo, cleanupDB := setupTestRepository(t)
	defer cleanupDB()

	mockRPC := createMockRPCClient()
	rangeSize := uint64(100)
	server := NewTestServer(repo, mockRPC, rangeSize)

	// Create a test router for the server
	router := createTestRouter(server)

	// Setup test data
	setupTestData(t, repo)

	t.Run("Concurrent analytics requests", func(t *testing.T) {
		concurrency := 10
		done := make(chan bool, concurrency)

		for i := 0; i < concurrency; i++ {
			go func(i int) {
				defer func() { done <- true }()

				expiryBlock := 100 + i*10
				req, err := http.NewRequest("GET", fmt.Sprintf("/api/v1/stats/analytics?expiry_block=%d", expiryBlock), nil)
				require.NoError(t, err)

				rr := httptest.NewRecorder()
				router.ServeHTTP(rr, req)

				assert.Equal(t, http.StatusOK, rr.Code)

				var analytics repository.AnalyticsData
				err = json.Unmarshal(rr.Body.Bytes(), &analytics)
				assert.NoError(t, err)
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < concurrency; i++ {
			<-done
		}
	})

	t.Run("Concurrent sync requests", func(t *testing.T) {
		concurrency := 10
		done := make(chan bool, concurrency)

		for i := 0; i < concurrency; i++ {
			go func() {
				defer func() { done <- true }()

				req, err := http.NewRequest("GET", "/api/v1/sync", nil)
				require.NoError(t, err)

				rr := httptest.NewRecorder()
				router.ServeHTTP(rr, req)

				assert.Equal(t, http.StatusOK, rr.Code)

				var syncStatus repository.SyncStatus
				err = json.Unmarshal(rr.Body.Bytes(), &syncStatus)
				assert.NoError(t, err)
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < concurrency; i++ {
			<-done
		}
	})
}

// testHealthCheckEndpoint tests the health check endpoint
func testHealthCheckEndpoint(t *testing.T, server *TestServer) {
	router := createTestRouter(server)

	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "State Expiry API", rr.Body.String())
}

// testAnalyticsEndpoint tests the analytics endpoint
func testAnalyticsEndpoint(t *testing.T, server *TestServer) {
	router := createTestRouter(server)

	// Test with valid expiry_block parameter
	req, err := http.NewRequest("GET", "/api/v1/stats/analytics?expiry_block=100", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var analytics repository.AnalyticsData
	err = json.Unmarshal(rr.Body.Bytes(), &analytics)
	require.NoError(t, err)

	// Verify analytics data structure
	assert.NotNil(t, analytics.AccountExpiry)
	assert.NotNil(t, analytics.AccountDistribution)
	assert.NotNil(t, analytics.StorageSlotExpiry)
	assert.NotNil(t, analytics.ContractStorage)
	assert.NotNil(t, analytics.StorageExpiry)
	assert.NotNil(t, analytics.FullyExpiredContracts)
	assert.NotNil(t, analytics.ActiveContractsExpiredStorage)
	assert.NotNil(t, analytics.CompleteExpiry)

	// Test with different expiry_block values
	testCases := []uint64{50, 150, 200, 500}
	for _, expiryBlock := range testCases {
		req, err := http.NewRequest("GET", fmt.Sprintf("/api/v1/stats/analytics?expiry_block=%d", expiryBlock), nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var analytics repository.AnalyticsData
		err = json.Unmarshal(rr.Body.Bytes(), &analytics)
		require.NoError(t, err)

		// Verify response structure is consistent
		assert.NotNil(t, analytics.AccountExpiry)
		assert.NotNil(t, analytics.StorageSlotExpiry)
	}
}

// testSyncEndpoint tests the sync endpoint
func testSyncEndpoint(t *testing.T, server *TestServer) {
	router := createTestRouter(server)

	req, err := http.NewRequest("GET", "/api/v1/sync", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var syncStatus repository.SyncStatus
	err = json.Unmarshal(rr.Body.Bytes(), &syncStatus)
	require.NoError(t, err)

	// Verify sync status structure
	assert.GreaterOrEqual(t, syncStatus.LastIndexedRange, uint64(0))
	assert.GreaterOrEqual(t, syncStatus.EndBlock, uint64(0))
}

// Helper functions

// TestServer is a test-friendly version of Server that works with interfaces
type TestServer struct {
	repo      repository.StateRepositoryInterface
	rpcClient rpc.ClientInterface
	rangeSize uint64
	log       *slog.Logger
}

// NewTestServer creates a new test server with mock RPC client
func NewTestServer(repo repository.StateRepositoryInterface, rpcClient rpc.ClientInterface, rangeSize uint64) *TestServer {
	return &TestServer{
		repo:      repo,
		rpcClient: rpcClient,
		rangeSize: rangeSize,
		log:       logger.GetLogger("api-server-test"),
	}
}

// handleGetSyncStatus handles the sync status endpoint for testing
func (s *TestServer) handleGetSyncStatus(w http.ResponseWriter, r *http.Request) {
	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	latestBlock := latestBlockBig.Uint64()

	// Calculate the latest range number
	// Range calculation: for block 0 (genesis) = range 0, for others = (blockNumber - 1) / rangeSize
	var latestRange uint64
	if latestBlock == 0 {
		latestRange = 0
	} else {
		latestRange = (latestBlock-1)/s.rangeSize + 1
	}

	// Get sync status from repository
	syncStatus, err := s.repo.GetSyncStatus(r.Context(), latestRange, s.rangeSize)
	if err != nil {
		s.log.Error("Failed to get sync status",
			"error", err,
			"latest_range", latestRange,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get sync status")
		return
	}

	s.log.Debug("Served sync status",
		"latest_block", latestBlock,
		"latest_range", latestRange,
		"last_indexed_range", syncStatus.LastIndexedRange,
		"is_synced", syncStatus.IsSynced,
		"end_block", syncStatus.EndBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, syncStatus)
}

// handleGetAnalytics handles the analytics endpoint for testing
func (s *TestServer) handleGetAnalytics(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	currentBlock := latestBlockBig.Uint64()

	analytics, err := s.repo.GetAnalyticsData(r.Context(), expiryBlock, currentBlock)
	if err != nil {
		s.log.Error("Failed to get analytics data",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get analytics data")
		return
	}

	s.log.Debug("Served analytics data",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"expired_accounts", analytics.AccountExpiry.TotalExpiredAccounts,
		"total_accounts", analytics.AccountExpiry.TotalAccounts,
		"expired_slots", analytics.StorageSlotExpiry.ExpiredSlots,
		"total_slots", analytics.StorageSlotExpiry.TotalSlots,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// createMockRPCClient creates a mock RPC client for testing
func createMockRPCClient() rpc.ClientInterface {
	return &MockRPCWrapper{}
}

// We need to create a custom type that wraps the real client for mocking
type MockRPCWrapper struct {
	client *rpc.Client
}

func (m *MockRPCWrapper) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	return big.NewInt(1000), nil
}

func (m *MockRPCWrapper) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	return "0x", nil
}

func (m *MockRPCWrapper) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]rpc.TransactionResult, error) {
	return []rpc.TransactionResult{}, nil
}

// FailingRPCWrapper provides an RPC client that always fails
type FailingRPCWrapper struct{}

func (f *FailingRPCWrapper) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	return nil, fmt.Errorf("RPC client failure")
}

func (f *FailingRPCWrapper) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	return "", fmt.Errorf("RPC client failure")
}

func (f *FailingRPCWrapper) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]rpc.TransactionResult, error) {
	return nil, fmt.Errorf("RPC client failure")
}

// createTestRouter creates a test router for the server
func createTestRouter(server *TestServer) http.Handler {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("State Expiry API"))
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/stats/analytics", server.handleGetAnalytics)
		r.Get("/sync", server.handleGetSyncStatus)
	})

	return r
}

// setupTestRepository creates a test repository with database setup
func setupTestRepository(t *testing.T) (repository.StateRepositoryInterface, func()) {
	t.Helper()

	cleanup := testdb.SetupTestDatabase(t)

	// Get test configuration
	testConfig := testdb.GetTestConfig()

	config := internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
	}

	ctx := context.Background()
	repo, err := repository.NewRepository(ctx, config)
	require.NoError(t, err, "Failed to create repository")

	return repo, cleanup
}

// setupTestData creates test data for API endpoint testing
func setupTestData(t *testing.T, repo repository.StateRepositoryInterface) {
	t.Helper()

	ctx := context.Background()

	// Create test data for analytics
	// For ClickHouse, use archive mode data structure
	accountsByBlock := map[uint64]map[string]struct{}{
		100: {
			"0x1111111111111111111111111111111111111111": {},
			"0x2222222222222222222222222222222222222222": {},
		},
		200: {
			"0x3333333333333333333333333333333333333333": {},
			"0x4444444444444444444444444444444444444444": {},
		},
	}

	accountTypes := map[string]bool{
		"0x1111111111111111111111111111111111111111": false, // EOA
		"0x2222222222222222222222222222222222222222": true,  // Contract
		"0x3333333333333333333333333333333333333333": false, // EOA
		"0x4444444444444444444444444444444444444444": true,  // Contract
	}

	storageByBlock := map[uint64]map[string]map[string]struct{}{
		100: {
			"0x2222222222222222222222222222222222222222": {
				"0x0000000000000000000000000000000000000000000000000000000000000001": {},
				"0x0000000000000000000000000000000000000000000000000000000000000002": {},
			},
		},
		200: {
			"0x4444444444444444444444444444444444444444": {
				"0x0000000000000000000000000000000000000000000000000000000000000003": {},
			},
		},
	}

	err := repo.InsertRange(ctx, accountsByBlock, accountTypes, storageByBlock, 1)
	require.NoError(t, err, "Failed to setup test data for archive mode")
}
