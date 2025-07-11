package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"
)

// getTestClickHouseConfig returns test ClickHouse configuration matching testdb package
func getTestClickHouseConfig() internal.Config {
	return internal.Config{
		ClickHouseHost:     "localhost",
		ClickHousePort:     "19010",
		ClickHouseUser:     "test_user",
		ClickHousePassword: "test_password",
		ClickHouseDatabase: "test_state_expiry",
		ClickHouseMaxConns: 5,
		ClickHouseMinConns: 1,
		Environment:        "test",
		RPCURLS:            []string{"http://localhost:8545"}, // Required for validation
	}
}

// ==============================================================================
// ANALYTICS TEST DATA GENERATORS
// ==============================================================================

// AnalyticsTestDataConfig configures test data generation for analytics testing
type AnalyticsTestDataConfig struct {
	// Account configuration
	NumEOAs      int
	NumContracts int

	// Storage configuration
	SlotsPerContract    int
	MaxSlotsPerContract int

	// Block range configuration
	StartBlock  uint64
	EndBlock    uint64
	ExpiryBlock uint64

	// Access patterns
	SingleAccessAccountsPercent float64 // Percentage of accounts with single access
	SingleAccessSlotsPercent    float64 // Percentage of storage slots with single access

	// Activity patterns
	HighActivityBlocks []uint64 // Blocks with high activity
	LowActivityBlocks  []uint64 // Blocks with low activity

	// Randomization
	RandomSeed int64
}

// DefaultAnalyticsTestDataConfig returns a standard configuration for analytics testing
func DefaultAnalyticsTestDataConfig() AnalyticsTestDataConfig {
	return AnalyticsTestDataConfig{
		NumEOAs:                     100,
		NumContracts:                50,
		SlotsPerContract:            10,
		MaxSlotsPerContract:         100,
		StartBlock:                  1,
		EndBlock:                    1000,
		ExpiryBlock:                 500,
		SingleAccessAccountsPercent: 0.2, // 20% single access
		SingleAccessSlotsPercent:    0.3, // 30% single access
		HighActivityBlocks:          []uint64{100, 200, 300},
		LowActivityBlocks:           []uint64{50, 150, 250},
		RandomSeed:                  42,
	}
}

// GenerateAnalyticsTestData creates comprehensive test data for analytics testing
func GenerateAnalyticsTestData(config AnalyticsTestDataConfig) *AnalyticsTestData {
	rand.Seed(config.RandomSeed)

	data := &AnalyticsTestData{
		Config:          config,
		EOAs:            make([]string, config.NumEOAs),
		Contracts:       make([]string, config.NumContracts),
		AccountAccesses: make(map[uint64]map[string]struct{}),
		AccountTypes:    make(map[string]bool),
		StorageAccesses: make(map[uint64]map[string]map[string]struct{}),
		StorageSlots:    make(map[string][]string),
	}

	// Generate EOA addresses
	for i := 0; i < config.NumEOAs; i++ {
		addr := fmt.Sprintf("0x%040x", i+1)
		data.EOAs[i] = addr
		data.AccountTypes[addr] = false // EOA
	}

	// Generate contract addresses
	for i := 0; i < config.NumContracts; i++ {
		addr := fmt.Sprintf("0x%040x", i+1000)
		data.Contracts[i] = addr
		data.AccountTypes[addr] = true // Contract

		// Generate storage slots for each contract
		numSlots := config.SlotsPerContract
		if config.MaxSlotsPerContract > config.SlotsPerContract {
			numSlots = config.SlotsPerContract + rand.Intn(config.MaxSlotsPerContract-config.SlotsPerContract)
		}

		slots := make([]string, numSlots)
		for j := 0; j < numSlots; j++ {
			slots[j] = fmt.Sprintf("0x%064x", j+1)
		}
		data.StorageSlots[addr] = slots
	}

	// Generate account accesses
	data.generateAccountAccesses()

	// Generate storage accesses
	data.generateStorageAccesses()

	return data
}

// AnalyticsTestData holds comprehensive test data for analytics testing
type AnalyticsTestData struct {
	Config          AnalyticsTestDataConfig
	EOAs            []string
	Contracts       []string
	AccountAccesses map[uint64]map[string]struct{}
	AccountTypes    map[string]bool
	StorageAccesses map[uint64]map[string]map[string]struct{}
	StorageSlots    map[string][]string
}

// generateAccountAccesses creates account access patterns based on configuration
func (data *AnalyticsTestData) generateAccountAccesses() {
	config := data.Config

	// Initialize account accesses map
	for block := config.StartBlock; block <= config.EndBlock; block++ {
		data.AccountAccesses[block] = make(map[string]struct{})
	}

	// Generate single access accounts
	singleAccessEOAs := int(float64(len(data.EOAs)) * config.SingleAccessAccountsPercent)
	singleAccessContracts := int(float64(len(data.Contracts)) * config.SingleAccessAccountsPercent)

	// Single access EOAs
	for i := 0; i < singleAccessEOAs; i++ {
		addr := data.EOAs[i]
		block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))
		data.AccountAccesses[block][addr] = struct{}{}
	}

	// Single access contracts
	for i := 0; i < singleAccessContracts; i++ {
		addr := data.Contracts[i]
		block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))
		data.AccountAccesses[block][addr] = struct{}{}
	}

	// Generate multiple access accounts
	multiAccessEOAs := data.EOAs[singleAccessEOAs:]
	multiAccessContracts := data.Contracts[singleAccessContracts:]

	// Multiple access EOAs
	for _, addr := range multiAccessEOAs {
		numAccesses := 2 + rand.Intn(5) // 2-6 accesses
		for j := 0; j < numAccesses; j++ {
			block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))
			data.AccountAccesses[block][addr] = struct{}{}
		}
	}

	// Multiple access contracts
	for _, addr := range multiAccessContracts {
		numAccesses := 2 + rand.Intn(8) // 2-9 accesses
		for j := 0; j < numAccesses; j++ {
			block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))
			data.AccountAccesses[block][addr] = struct{}{}
		}
	}

	// Add high activity to specific blocks
	for _, block := range config.HighActivityBlocks {
		if block >= config.StartBlock && block <= config.EndBlock {
			// Add extra accesses for high activity blocks
			for i := 0; i < 20; i++ {
				if i < len(data.EOAs) {
					data.AccountAccesses[block][data.EOAs[i]] = struct{}{}
				}
				if i < len(data.Contracts) {
					data.AccountAccesses[block][data.Contracts[i]] = struct{}{}
				}
			}
		}
	}
}

// generateStorageAccesses creates storage access patterns based on configuration
func (data *AnalyticsTestData) generateStorageAccesses() {
	config := data.Config

	// Initialize storage accesses map
	for block := config.StartBlock; block <= config.EndBlock; block++ {
		data.StorageAccesses[block] = make(map[string]map[string]struct{})
	}

	// Generate storage accesses for each contract
	for contractAddr, slots := range data.StorageSlots {
		// Determine single access slots
		singleAccessSlots := int(float64(len(slots)) * config.SingleAccessSlotsPercent)

		// Single access slots
		for i := 0; i < singleAccessSlots && i < len(slots); i++ {
			slot := slots[i]
			block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))

			if data.StorageAccesses[block][contractAddr] == nil {
				data.StorageAccesses[block][contractAddr] = make(map[string]struct{})
			}
			data.StorageAccesses[block][contractAddr][slot] = struct{}{}
		}

		// Multiple access slots
		for i := singleAccessSlots; i < len(slots); i++ {
			slot := slots[i]
			numAccesses := 2 + rand.Intn(4) // 2-5 accesses

			for j := 0; j < numAccesses; j++ {
				block := config.StartBlock + uint64(rand.Intn(int(config.EndBlock-config.StartBlock+1)))

				if data.StorageAccesses[block][contractAddr] == nil {
					data.StorageAccesses[block][contractAddr] = make(map[string]struct{})
				}
				data.StorageAccesses[block][contractAddr][slot] = struct{}{}
			}
		}
	}

	// Add high activity to specific blocks
	for _, block := range config.HighActivityBlocks {
		if block >= config.StartBlock && block <= config.EndBlock {
			// Add extra storage accesses for high activity blocks
			for i := 0; i < 10 && i < len(data.Contracts); i++ {
				contractAddr := data.Contracts[i]
				slots := data.StorageSlots[contractAddr]

				if data.StorageAccesses[block][contractAddr] == nil {
					data.StorageAccesses[block][contractAddr] = make(map[string]struct{})
				}

				for j := 0; j < 5 && j < len(slots); j++ {
					data.StorageAccesses[block][contractAddr][slots[j]] = struct{}{}
				}
			}
		}
	}
}

// InsertTestData inserts the generated test data into the repository
func (data *AnalyticsTestData) InsertTestData(ctx context.Context, repo StateRepositoryInterface) error {
	// Insert data in chunks to avoid overwhelming the database
	const chunkSize = 100

	for block := data.Config.StartBlock; block <= data.Config.EndBlock; block += chunkSize {
		endBlock := block + chunkSize - 1
		if endBlock > data.Config.EndBlock {
			endBlock = data.Config.EndBlock
		}

		// Prepare chunk data
		chunkAccountAccesses := make(map[uint64]map[string]struct{})
		chunkStorageAccesses := make(map[uint64]map[string]map[string]struct{})

		for b := block; b <= endBlock; b++ {
			if accounts, exists := data.AccountAccesses[b]; exists {
				chunkAccountAccesses[b] = accounts
			}
			if storage, exists := data.StorageAccesses[b]; exists {
				chunkStorageAccesses[b] = storage
			}
		}

		// Insert chunk
		if err := repo.InsertRange(ctx, chunkAccountAccesses, data.AccountTypes, chunkStorageAccesses, endBlock); err != nil {
			return fmt.Errorf("failed to insert test data chunk %d-%d: %w", block, endBlock, err)
		}
	}

	return nil
}

// ==============================================================================
// PERFORMANCE BENCHMARKING UTILITIES
// ==============================================================================

// BenchmarkResult holds the results of a performance benchmark
type BenchmarkResult struct {
	MethodName    string
	ExecutionTime time.Duration
	MemoryUsage   int64
	QueryCount    int
	RowsProcessed int64
	Success       bool
	Error         error
}

// PerformanceBenchmark runs a performance benchmark for an analytics method
func PerformanceBenchmark(t *testing.T, name string, fn func() error) *BenchmarkResult {
	t.Helper()

	result := &BenchmarkResult{
		MethodName: name,
	}

	start := time.Now()
	err := fn()
	result.ExecutionTime = time.Since(start)
	result.Error = err
	result.Success = err == nil

	return result
}

// BenchmarkSuite holds multiple benchmark results
type BenchmarkSuite struct {
	Results []BenchmarkResult
}

// Add adds a benchmark result to the suite
func (bs *BenchmarkSuite) Add(result *BenchmarkResult) {
	bs.Results = append(bs.Results, *result)
}

// Report generates a performance report for all benchmarks
func (bs *BenchmarkSuite) Report(t *testing.T) {
	t.Helper()

	t.Logf("=== Performance Benchmark Report ===")
	for _, result := range bs.Results {
		status := "✅ PASS"
		if !result.Success {
			status = "❌ FAIL"
		}

		t.Logf("%s %s: %v", status, result.MethodName, result.ExecutionTime)
		if result.Error != nil {
			t.Logf("  Error: %v", result.Error)
		}
	}
}

// ==============================================================================
// ERROR SCENARIO SIMULATION UTILITIES
// ==============================================================================

// ErrorScenario represents different error conditions for testing
type ErrorScenario struct {
	Name        string
	Description string
	SimulateErr func() error
}

// Common error scenarios for analytics testing
var CommonErrorScenarios = []ErrorScenario{
	{
		Name:        "DatabaseConnectionFailure",
		Description: "Simulates database connection failure",
		SimulateErr: func() error {
			return sql.ErrConnDone
		},
	},
	{
		Name:        "QueryTimeout",
		Description: "Simulates query timeout",
		SimulateErr: func() error {
			return context.DeadlineExceeded
		},
	},
	{
		Name:        "InvalidParameters",
		Description: "Simulates invalid parameter handling",
		SimulateErr: func() error {
			return fmt.Errorf("invalid parameter: expiry_block cannot be greater than current_block")
		},
	},
}

// TestErrorScenario tests an analytics method against a specific error scenario
func TestErrorScenario(t *testing.T, scenario ErrorScenario, testFn func() error) {
	t.Helper()

	t.Run(scenario.Name, func(t *testing.T) {
		err := testFn()
		if err == nil {
			t.Errorf("Expected error for scenario %s, but got nil", scenario.Name)
		}
		t.Logf("Error scenario %s: %v", scenario.Name, err)
	})
}

// ==============================================================================
// ENHANCED TEST SETUP AND TEARDOWN
// ==============================================================================

// AnalyticsTestSetup provides comprehensive setup for analytics testing
type AnalyticsTestSetup struct {
	Repository StateRepositoryInterface
	TestData   *AnalyticsTestData
	Config     AnalyticsTestDataConfig
	Cleanup    func()
}

// SetupAnalyticsTest creates a complete analytics test environment
func SetupAnalyticsTest(t *testing.T, config AnalyticsTestDataConfig) *AnalyticsTestSetup {
	t.Helper()

	// Setup database
	dbConfig := getTestClickHouseConfig()
	cleanup := testdb.SetupTestDatabase(t)

	// Create repository
	repo, err := NewRepository(context.Background(), dbConfig)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Generate test data
	testData := GenerateAnalyticsTestData(config)

	// Insert test data
	ctx := context.Background()
	if err := testData.InsertTestData(ctx, repo); err != nil {
		cleanup()
		t.Fatalf("Failed to insert test data: %v", err)
	}

	return &AnalyticsTestSetup{
		Repository: repo,
		TestData:   testData,
		Config:     config,
		Cleanup:    cleanup,
	}
}

// SetupAnalyticsTestWithDefaults creates an analytics test environment with default configuration
func SetupAnalyticsTestWithDefaults(t *testing.T) *AnalyticsTestSetup {
	t.Helper()
	return SetupAnalyticsTest(t, DefaultAnalyticsTestDataConfig())
}

// ==============================================================================
// ANALYTICS VALIDATION UTILITIES
// ==============================================================================

// ValidateQueryParams validates QueryParams for analytics methods
func ValidateQueryParams(params QueryParams) error {
	if params.ExpiryBlock > params.CurrentBlock {
		return fmt.Errorf("expiry_block (%d) cannot be greater than current_block (%d)", params.ExpiryBlock, params.CurrentBlock)
	}

	if params.StartBlock > params.EndBlock {
		return fmt.Errorf("start_block (%d) cannot be greater than end_block (%d)", params.StartBlock, params.EndBlock)
	}

	if params.WindowSize <= 0 {
		return fmt.Errorf("window_size must be positive, got %d", params.WindowSize)
	}

	if params.TopN <= 0 {
		return fmt.Errorf("top_n must be positive, got %d", params.TopN)
	}

	return nil
}

// AssertAnalyticsDataConsistency validates that analytics data is internally consistent
func AssertAnalyticsDataConsistency(t *testing.T, data interface{}) {
	t.Helper()

	switch v := data.(type) {
	case *AccountAnalytics:
		// Validate account totals
		if v.Total.Total != v.Total.EOAs+v.Total.Contracts {
			t.Errorf("Account total mismatch: Total=%d, EOAs=%d, Contracts=%d", v.Total.Total, v.Total.EOAs, v.Total.Contracts)
		}

		// Validate expiry data
		if v.Expiry.TotalExpired != v.Expiry.ExpiredEOAs+v.Expiry.ExpiredContracts {
			t.Errorf("Expiry total mismatch: TotalExpired=%d, ExpiredEOAs=%d, ExpiredContracts=%d",
				v.Expiry.TotalExpired, v.Expiry.ExpiredEOAs, v.Expiry.ExpiredContracts)
		}

		// Validate rates
		if v.Total.Total > 0 {
			expectedExpiryRate := float64(v.Expiry.TotalExpired) / float64(v.Total.Total)
			if abs(v.Expiry.ExpiryRate-expectedExpiryRate) > 0.01 {
				t.Errorf("Expiry rate mismatch: Expected=%.2f, Got=%.2f", expectedExpiryRate, v.Expiry.ExpiryRate)
			}
		}

	case *StorageAnalytics:
		// Validate storage totals
		if v.Total.TotalSlots != v.Expiry.ExpiredSlots+v.Expiry.ActiveSlots {
			t.Errorf("Storage total mismatch: TotalSlots=%d, ExpiredSlots=%d, ActiveSlots=%d",
				v.Total.TotalSlots, v.Expiry.ExpiredSlots, v.Expiry.ActiveSlots)
		}

	case *UnifiedAnalytics:
		// Validate unified analytics consistency
		AssertAnalyticsDataConsistency(t, &v.Accounts)
		AssertAnalyticsDataConsistency(t, &v.Storage)

	default:
		t.Logf("Analytics data consistency validation not implemented for type %T", data)
	}
}

// Helper function for floating point comparison
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
