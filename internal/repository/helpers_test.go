package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnalyticsTestDataGeneration tests the analytics test data generation infrastructure
func TestAnalyticsTestDataGeneration(t *testing.T) {
	t.Run("DefaultConfiguration", func(t *testing.T) {
		config := DefaultAnalyticsTestDataConfig()

		// Validate default configuration
		assert.Equal(t, 100, config.NumEOAs)
		assert.Equal(t, 50, config.NumContracts)
		assert.Equal(t, 10, config.SlotsPerContract)
		assert.Equal(t, uint64(500), config.ExpiryBlock)
	})

	t.Run("TestDataGeneration", func(t *testing.T) {
		config := AnalyticsTestDataConfig{
			NumEOAs:          10,
			NumContracts:     5,
			SlotsPerContract: 3,
			StartBlock:       1,
			EndBlock:         100,
			ExpiryBlock:      50,
		}

		data := GenerateAnalyticsTestData(config)

		// Validate generated data structure
		assert.Equal(t, 10, len(data.EOAs))
		assert.Equal(t, 5, len(data.Contracts))
		assert.Equal(t, 15, len(data.AccountTypes)) // 10 EOAs + 5 contracts

		// Validate account types
		for _, addr := range data.EOAs {
			assert.False(t, data.AccountTypes[addr], "EOA should be marked as false")
		}
		for _, addr := range data.Contracts {
			assert.True(t, data.AccountTypes[addr], "Contract should be marked as true")
		}

		// Validate storage slots
		assert.Equal(t, 5, len(data.StorageSlots))
		for contractAddr, slots := range data.StorageSlots {
			assert.True(t, data.AccountTypes[contractAddr], "Storage slots should only exist for contracts")
			assert.GreaterOrEqual(t, len(slots), 3, "Should have at least SlotsPerContract slots")
			assert.LessOrEqual(t, len(slots), 5, "Should not exceed MaxSlotsPerContract slots")
		}

		// Validate account accesses
		totalAccountAccesses := 0
		for block := config.StartBlock; block <= config.EndBlock; block++ {
			if accesses, exists := data.AccountAccesses[block]; exists {
				totalAccountAccesses += len(accesses)
			}
		}
		assert.Greater(t, totalAccountAccesses, 0, "Should have account accesses")

		// Validate storage accesses
		totalStorageAccesses := 0
		for block := config.StartBlock; block <= config.EndBlock; block++ {
			if storageByBlock, exists := data.StorageAccesses[block]; exists {
				for _, slots := range storageByBlock {
					totalStorageAccesses += len(slots)
				}
			}
		}
		assert.Greater(t, totalStorageAccesses, 0, "Should have storage accesses")
	})

	t.Run("TestDataInsertion", func(t *testing.T) {
		// Use small dataset for quick testing
		config := AnalyticsTestDataConfig{
			NumEOAs:          5,
			NumContracts:     3,
			SlotsPerContract: 2,
			StartBlock:       1,
			EndBlock:         10,
			ExpiryBlock:      5,
		}

		setup := SetupAnalyticsTest(t, config)
		defer setup.Cleanup()

		// Validate that data was inserted successfully
		ctx := context.Background()

		// Test basic stats to ensure data is present
		stats, err := setup.Repository.GetBasicStats(ctx, config.ExpiryBlock)
		require.NoError(t, err)

		// Should have some accounts
		totalAccounts := stats.Accounts.TotalEOAs + stats.Accounts.TotalContracts
		assert.Greater(t, totalAccounts, 0, "Should have accounts in database")

		// Should have some storage
		assert.Greater(t, stats.Storage.TotalSlots, 0, "Should have storage slots in database")

		t.Logf("Inserted test data: %d accounts, %d storage slots", totalAccounts, stats.Storage.TotalSlots)
	})
}

// TestAnalyticsTestInfrastructure tests the testing infrastructure utilities
func TestAnalyticsTestInfrastructure(t *testing.T) {
	t.Run("QueryParamsValidation", func(t *testing.T) {
		// Valid params
		validParams := QueryParams{
			ExpiryBlock:  100,
			CurrentBlock: 200,
			StartBlock:   50,
			EndBlock:     150,
			WindowSize:   10,
			TopN:         5,
		}
		err := ValidateQueryParams(validParams)
		assert.NoError(t, err)

		// Invalid params - expiry after current
		invalidParams1 := QueryParams{
			ExpiryBlock:  200,
			CurrentBlock: 100,
			StartBlock:   50,
			EndBlock:     150,
			WindowSize:   10,
			TopN:         5,
		}
		err = ValidateQueryParams(invalidParams1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expiry_block")

		// Invalid params - start after end
		invalidParams2 := QueryParams{
			ExpiryBlock:  100,
			CurrentBlock: 200,
			StartBlock:   150,
			EndBlock:     50,
			WindowSize:   10,
			TopN:         5,
		}
		err = ValidateQueryParams(invalidParams2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "start_block")

		// Invalid params - zero window size
		invalidParams3 := QueryParams{
			ExpiryBlock:  100,
			CurrentBlock: 200,
			StartBlock:   50,
			EndBlock:     150,
			WindowSize:   0,
			TopN:         5,
		}
		err = ValidateQueryParams(invalidParams3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "window_size")

		// Invalid params - zero top N
		invalidParams4 := QueryParams{
			ExpiryBlock:  100,
			CurrentBlock: 200,
			StartBlock:   50,
			EndBlock:     150,
			WindowSize:   10,
			TopN:         0,
		}
		err = ValidateQueryParams(invalidParams4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "top_n")
	})

	t.Run("PerformanceBenchmarking", func(t *testing.T) {
		// Test successful benchmark
		result := PerformanceBenchmark(t, "TestMethod", func() error {
			// Simulate some work
			for i := 0; i < 1000; i++ {
				_ = i * i
			}
			return nil
		})

		assert.Equal(t, "TestMethod", result.MethodName)
		assert.True(t, result.Success)
		assert.NoError(t, result.Error)
		assert.Greater(t, result.ExecutionTime, time.Duration(0))

		// Test failed benchmark
		testError := fmt.Errorf("test error")
		result2 := PerformanceBenchmark(t, "FailingMethod", func() error {
			return testError
		})

		assert.Equal(t, "FailingMethod", result2.MethodName)
		assert.False(t, result2.Success)
		assert.Error(t, result2.Error)

		// Test benchmark suite
		suite := BenchmarkSuite{}
		suite.Add(result)
		suite.Add(result2)

		assert.Equal(t, 2, len(suite.Results))

		// Generate report (just ensure it doesn't panic)
		suite.Report(t)
	})

	t.Run("ErrorScenarios", func(t *testing.T) {
		// Test error scenario simulation
		for _, scenario := range CommonErrorScenarios {
			TestErrorScenario(t, scenario, func() error {
				return scenario.SimulateErr()
			})
		}
	})
}

// TestAnalyticsDataConsistency tests the analytics data consistency validation
func TestAnalyticsDataConsistency(t *testing.T) {
	t.Run("ValidAccountAnalytics", func(t *testing.T) {
		validData := &AccountAnalytics{
			Total: AccountTotals{
				EOAs:      60,
				Contracts: 40,
				Total:     100,
			},
			Expiry: AccountExpiryData{
				ExpiredEOAs:      20,
				ExpiredContracts: 10,
				TotalExpired:     30,
				ExpiryRate:       30.0,
			},
			SingleAccess: AccountSingleAccessData{
				SingleAccessEOAs:      15,
				SingleAccessContracts: 5,
				TotalSingleAccess:     20,
				SingleAccessRate:      0.20,
			},
			Distribution: AccountDistribution{
				EOAPercentage:      60.0,
				ContractPercentage: 40.0,
			},
		}

		// Should not fail validation
		AssertAnalyticsDataConsistency(t, validData)
	})

	t.Run("ValidStorageAnalytics", func(t *testing.T) {
		validData := &StorageAnalytics{
			Total: StorageTotals{
				TotalSlots: 100,
			},
			Expiry: StorageExpiryData{
				ExpiredSlots: 30,
				ActiveSlots:  70,
				ExpiryRate:   0.30,
			},
			SingleAccess: StorageSingleAccessData{
				SingleAccessSlots: 25,
				SingleAccessRate:  0.25,
			},
		}

		// Should not fail validation
		AssertAnalyticsDataConsistency(t, validData)
	})
}
