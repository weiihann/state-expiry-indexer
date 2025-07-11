package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	SlotsPerContract int

	// Block range configuration
	StartBlock  uint64
	EndBlock    uint64
	ExpiryBlock uint64
}

// DefaultAnalyticsTestDataConfig returns a standard configuration for analytics testing
func DefaultAnalyticsTestDataConfig() AnalyticsTestDataConfig {
	return AnalyticsTestDataConfig{
		NumEOAs:          100,
		NumContracts:     50,
		SlotsPerContract: 10,
		StartBlock:       1,
		EndBlock:         1000,
		ExpiryBlock:      500,
	}
}

// GenerateAnalyticsTestData creates comprehensive test data for analytics testing
func GenerateAnalyticsTestData(config AnalyticsTestDataConfig) *AnalyticsTestData {
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

		slots := make([]string, numSlots)
		for j := 0; j < numSlots; j++ {
			slots[j] = fmt.Sprintf("0x%064x", j+1)
		}
		data.StorageSlots[addr] = slots
	}

	// Generate deterministic account accesses
	data.generateAccountAccesses()

	// Generate deterministic storage accesses
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

// generateAccountAccesses creates deterministic account access patterns
func (data *AnalyticsTestData) generateAccountAccesses() {
	config := data.Config

	// Initialize account accesses map
	for block := config.StartBlock; block <= config.EndBlock; block++ {
		data.AccountAccesses[block] = make(map[string]struct{})
	}

	// Generate deterministic access patterns for all accounts
	// Each account gets accessed at predictable intervals
	blockRange := config.EndBlock - config.StartBlock + 1

	// EOAs get accessed every few blocks deterministically
	for i, addr := range data.EOAs {
		// Each EOA gets accessed at deterministic blocks based on its index
		accessBlock := config.StartBlock + uint64(i)%blockRange
		data.AccountAccesses[accessBlock][addr] = struct{}{}

		// Add a second access for some EOAs to create variety
		if i%3 == 0 && blockRange > 1 {
			secondBlock := config.StartBlock + uint64(i+1)%blockRange
			data.AccountAccesses[secondBlock][addr] = struct{}{}
		}
	}

	// Contracts get accessed at different deterministic intervals
	for i, addr := range data.Contracts {
		// Each contract gets accessed at deterministic blocks based on its index
		accessBlock := config.StartBlock + uint64(i*2)%blockRange
		data.AccountAccesses[accessBlock][addr] = struct{}{}

		// Add additional accesses for contracts to simulate more activity
		if blockRange > 2 {
			secondBlock := config.StartBlock + uint64(i*2+1)%blockRange
			data.AccountAccesses[secondBlock][addr] = struct{}{}
		}
		if i%2 == 0 && blockRange > 3 {
			thirdBlock := config.StartBlock + uint64(i*3)%blockRange
			data.AccountAccesses[thirdBlock][addr] = struct{}{}
		}
	}
}

// generateStorageAccesses creates deterministic storage access patterns
func (data *AnalyticsTestData) generateStorageAccesses() {
	config := data.Config

	// Initialize storage accesses map
	for block := config.StartBlock; block <= config.EndBlock; block++ {
		data.StorageAccesses[block] = make(map[string]map[string]struct{})
	}

	// Generate deterministic storage accesses for each contract
	blockRange := config.EndBlock - config.StartBlock + 1

	for contractIndex, contractAddr := range data.Contracts {
		slots := data.StorageSlots[contractAddr]

		// Each storage slot gets accessed at deterministic intervals
		for slotIndex, slot := range slots {
			// Calculate deterministic access block based on contract and slot indices
			accessBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex))%blockRange

			if data.StorageAccesses[accessBlock][contractAddr] == nil {
				data.StorageAccesses[accessBlock][contractAddr] = make(map[string]struct{})
			}
			data.StorageAccesses[accessBlock][contractAddr][slot] = struct{}{}

			// Add additional accesses for some slots to create variety
			if slotIndex%2 == 0 && blockRange > 1 {
				secondBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex+1))%blockRange
				if data.StorageAccesses[secondBlock][contractAddr] == nil {
					data.StorageAccesses[secondBlock][contractAddr] = make(map[string]struct{})
				}
				data.StorageAccesses[secondBlock][contractAddr][slot] = struct{}{}
			}

			// Add third access for some slots
			if slotIndex%3 == 0 && blockRange > 2 {
				thirdBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex+2))%blockRange
				if data.StorageAccesses[thirdBlock][contractAddr] == nil {
					data.StorageAccesses[thirdBlock][contractAddr] = make(map[string]struct{})
				}
				data.StorageAccesses[thirdBlock][contractAddr][slot] = struct{}{}
			}
		}
	}
}

// InsertTestData inserts the generated test data into the repository
func (data *AnalyticsTestData) InsertTestData(ctx context.Context, repo StateRepositoryInterface) error {
	// Insert data in chunks to avoid overwhelming the database
	const chunkSize = 10

	for block := data.Config.StartBlock; block <= data.Config.EndBlock; block += chunkSize {
		endBlock := min(block+chunkSize-1, data.Config.EndBlock)

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
	repo, err := NewRepository(t.Context(), dbConfig)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Generate test data
	testData := GenerateAnalyticsTestData(config)

	// Insert test data
	ctx := t.Context()
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

		// Validate rates (implementation returns percentages, not decimals)
		if v.Total.Total > 0 {
			expectedExpiryRate := float64(v.Expiry.TotalExpired) / float64(v.Total.Total) * 100
			if abs(v.Expiry.ExpiryRate-expectedExpiryRate) > 0.1 {
				t.Errorf("Expiry rate mismatch: Expected=%.2f%%, Got=%.2f%%", expectedExpiryRate, v.Expiry.ExpiryRate)
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

// ==============================================================================
// DETERMINISTIC ANALYTICS CALCULATION UTILITIES
// ==============================================================================

// CalculateExpectedAccountAnalytics calculates the expected account analytics based on deterministic test data
func (data *AnalyticsTestData) CalculateExpectedAccountAnalytics(expiryBlock uint64) *AccountAnalytics {
	config := data.Config
	blockRange := config.EndBlock - config.StartBlock + 1

	// Calculate last access blocks for each account based on the deterministic generation logic
	accountLastAccess := make(map[string]uint64)

	// Calculate EOA last access blocks
	for i, addr := range data.EOAs {
		// Primary access
		accessBlock := config.StartBlock + uint64(i)%blockRange
		accountLastAccess[addr] = accessBlock

		// Secondary access (if applicable)
		if i%3 == 0 && blockRange > 1 {
			secondBlock := config.StartBlock + uint64(i+1)%blockRange
			if secondBlock > accountLastAccess[addr] {
				accountLastAccess[addr] = secondBlock
			}
		}
	}

	// Calculate contract last access blocks
	for i, addr := range data.Contracts {
		// Primary access
		accessBlock := config.StartBlock + uint64(i*2)%blockRange
		accountLastAccess[addr] = accessBlock

		// Secondary access
		if blockRange > 2 {
			secondBlock := config.StartBlock + uint64(i*2+1)%blockRange
			if secondBlock > accountLastAccess[addr] {
				accountLastAccess[addr] = secondBlock
			}
		}

		// Tertiary access (if applicable)
		if i%2 == 0 && blockRange > 3 {
			thirdBlock := config.StartBlock + uint64(i*3)%blockRange
			if thirdBlock > accountLastAccess[addr] {
				accountLastAccess[addr] = thirdBlock
			}
		}
	}

	// Calculate access counts for each account
	accountAccessCounts := make(map[string]int)
	for i, addr := range data.EOAs {
		count := 1 // Primary access
		if i%3 == 0 && blockRange > 1 {
			count++ // Secondary access
		}
		accountAccessCounts[addr] = count
	}

	for i, addr := range data.Contracts {
		count := 1 // Primary access
		if blockRange > 2 {
			count++ // Secondary access
		}
		if i%2 == 0 && blockRange > 3 {
			count++ // Tertiary access
		}
		accountAccessCounts[addr] = count
	}

	// Calculate expiry and single access statistics
	var expiredEOAs, expiredContracts, singleAccessEOAs, singleAccessContracts int

	for _, addr := range data.EOAs {
		if accountLastAccess[addr] < expiryBlock {
			expiredEOAs++
		}
		if accountAccessCounts[addr] == 1 {
			singleAccessEOAs++
		}
	}

	for _, addr := range data.Contracts {
		if accountLastAccess[addr] < expiryBlock {
			expiredContracts++
		}
		if accountAccessCounts[addr] == 1 {
			singleAccessContracts++
		}
	}

	totalExpired := expiredEOAs + expiredContracts
	totalSingleAccess := singleAccessEOAs + singleAccessContracts
	totalAccounts := len(data.EOAs) + len(data.Contracts)

	var expiryRate, singleAccessRate float64
	if totalAccounts > 0 {
		expiryRate = float64(totalExpired) / float64(totalAccounts) * 100
		singleAccessRate = float64(totalSingleAccess) / float64(totalAccounts) * 100
	}

	var eoaPercentage, contractPercentage float64
	if totalAccounts > 0 {
		eoaPercentage = float64(len(data.EOAs)) / float64(totalAccounts) * 100
		contractPercentage = float64(len(data.Contracts)) / float64(totalAccounts) * 100
	}

	return &AccountAnalytics{
		Total: AccountTotals{
			EOAs:      len(data.EOAs),
			Contracts: len(data.Contracts),
			Total:     totalAccounts,
		},
		Expiry: AccountExpiryData{
			ExpiredEOAs:      expiredEOAs,
			ExpiredContracts: expiredContracts,
			TotalExpired:     totalExpired,
			ExpiryRate:       expiryRate,
		},
		SingleAccess: AccountSingleAccessData{
			SingleAccessEOAs:      singleAccessEOAs,
			SingleAccessContracts: singleAccessContracts,
			TotalSingleAccess:     totalSingleAccess,
			SingleAccessRate:      singleAccessRate,
		},
		Distribution: AccountDistribution{
			EOAPercentage:      eoaPercentage,
			ContractPercentage: contractPercentage,
		},
	}
}

// CalculateExpectedStorageAnalytics calculates the expected storage analytics based on deterministic test data
func (data *AnalyticsTestData) CalculateExpectedStorageAnalytics(expiryBlock uint64) *StorageAnalytics {
	config := data.Config
	blockRange := config.EndBlock - config.StartBlock + 1

	// Calculate last access blocks for each storage slot based on the deterministic generation logic
	slotLastAccess := make(map[string]map[string]uint64) // contract -> slot -> lastBlock
	slotAccessCounts := make(map[string]map[string]int)  // contract -> slot -> count

	for contractIndex, contractAddr := range data.Contracts {
		slots := data.StorageSlots[contractAddr]
		slotLastAccess[contractAddr] = make(map[string]uint64)
		slotAccessCounts[contractAddr] = make(map[string]int)

		for slotIndex, slot := range slots {
			// Primary access
			accessBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex))%blockRange
			slotLastAccess[contractAddr][slot] = accessBlock
			count := 1

			// Secondary access (if applicable)
			if slotIndex%2 == 0 && blockRange > 1 {
				secondBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex+1))%blockRange
				if secondBlock > slotLastAccess[contractAddr][slot] {
					slotLastAccess[contractAddr][slot] = secondBlock
				}
				count++
			}

			// Tertiary access (if applicable)
			if slotIndex%3 == 0 && blockRange > 2 {
				thirdBlock := config.StartBlock + uint64((contractIndex*len(slots)+slotIndex+2))%blockRange
				if thirdBlock > slotLastAccess[contractAddr][slot] {
					slotLastAccess[contractAddr][slot] = thirdBlock
				}
				count++
			}

			slotAccessCounts[contractAddr][slot] = count
		}
	}

	// Calculate expiry and single access statistics
	var expiredSlots, singleAccessSlots, totalSlots int

	for contractAddr, slots := range data.StorageSlots {
		for _, slot := range slots {
			totalSlots++
			if slotLastAccess[contractAddr][slot] < expiryBlock {
				expiredSlots++
			}
			if slotAccessCounts[contractAddr][slot] == 1 {
				singleAccessSlots++
			}
		}
	}

	activeSlots := totalSlots - expiredSlots

	var expiryRate, singleAccessRate float64
	if totalSlots > 0 {
		expiryRate = float64(expiredSlots) / float64(totalSlots) * 100
		singleAccessRate = float64(singleAccessSlots) / float64(totalSlots) * 100
	}

	return &StorageAnalytics{
		Total: StorageTotals{
			TotalSlots: totalSlots,
		},
		Expiry: StorageExpiryData{
			ExpiredSlots: expiredSlots,
			ActiveSlots:  activeSlots,
			ExpiryRate:   expiryRate,
		},
		SingleAccess: StorageSingleAccessData{
			SingleAccessSlots: singleAccessSlots,
			SingleAccessRate:  singleAccessRate,
		},
	}
}

// AssertAccountAnalyticsMatch validates that actual analytics match expected analytics
func AssertAccountAnalyticsMatch(t *testing.T, expected, actual *AccountAnalytics, tolerance float64) {
	t.Helper()

	// Validate totals
	assert.Equal(t, expected.Total.EOAs, actual.Total.EOAs, "EOA count mismatch")
	assert.Equal(t, expected.Total.Contracts, actual.Total.Contracts, "Contract count mismatch")
	assert.Equal(t, expected.Total.Total, actual.Total.Total, "Total account count mismatch")

	// Validate expiry data
	assert.Equal(t, expected.Expiry.ExpiredEOAs, actual.Expiry.ExpiredEOAs, "Expired EOA count mismatch")
	assert.Equal(t, expected.Expiry.ExpiredContracts, actual.Expiry.ExpiredContracts, "Expired contract count mismatch")
	assert.Equal(t, expected.Expiry.TotalExpired, actual.Expiry.TotalExpired, "Total expired count mismatch")
	assert.InDelta(t, expected.Expiry.ExpiryRate, actual.Expiry.ExpiryRate, tolerance, "Expiry rate mismatch")

	// Validate single access data
	assert.Equal(t, expected.SingleAccess.SingleAccessEOAs, actual.SingleAccess.SingleAccessEOAs, "Single access EOA count mismatch")
	assert.Equal(t, expected.SingleAccess.SingleAccessContracts, actual.SingleAccess.SingleAccessContracts, "Single access contract count mismatch")
	assert.Equal(t, expected.SingleAccess.TotalSingleAccess, actual.SingleAccess.TotalSingleAccess, "Total single access count mismatch")
	assert.InDelta(t, expected.SingleAccess.SingleAccessRate, actual.SingleAccess.SingleAccessRate, tolerance, "Single access rate mismatch")

	// Validate distribution
	assert.InDelta(t, expected.Distribution.EOAPercentage, actual.Distribution.EOAPercentage, tolerance, "EOA percentage mismatch")
	assert.InDelta(t, expected.Distribution.ContractPercentage, actual.Distribution.ContractPercentage, tolerance, "Contract percentage mismatch")
}

// AssertStorageAnalyticsMatch validates that actual analytics match expected analytics
func AssertStorageAnalyticsMatch(t *testing.T, expected, actual *StorageAnalytics, tolerance float64) {
	t.Helper()

	// Validate totals
	assert.Equal(t, expected.Total.TotalSlots, actual.Total.TotalSlots, "Total slots count mismatch")

	// Validate expiry data
	assert.Equal(t, expected.Expiry.ExpiredSlots, actual.Expiry.ExpiredSlots, "Expired slots count mismatch")
	assert.Equal(t, expected.Expiry.ActiveSlots, actual.Expiry.ActiveSlots, "Active slots count mismatch")
	assert.InDelta(t, expected.Expiry.ExpiryRate, actual.Expiry.ExpiryRate, tolerance, "Expiry rate mismatch")

	// Validate single access data
	assert.Equal(t, expected.SingleAccess.SingleAccessSlots, actual.SingleAccess.SingleAccessSlots, "Single access slots count mismatch")
	assert.InDelta(t, expected.SingleAccess.SingleAccessRate, actual.SingleAccess.SingleAccessRate, tolerance, "Single access rate mismatch")
}
