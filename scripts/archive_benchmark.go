package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/database"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
)

// BenchmarkResult holds the results of a single benchmark test
type BenchmarkResult struct {
	TestName        string        `json:"test_name"`
	Database        string        `json:"database"`
	ExpiryBlock     uint64        `json:"expiry_block"`
	Duration        time.Duration `json:"duration"`
	Success         bool          `json:"success"`
	ErrorMessage    string        `json:"error_message,omitempty"`
	RecordsAffected int64         `json:"records_affected,omitempty"`
}

// BenchmarkSuite contains all benchmark results
type BenchmarkSuite struct {
	Timestamp   time.Time         `json:"timestamp"`
	Environment string            `json:"environment"`
	Results     []BenchmarkResult `json:"results"`
	Summary     BenchmarkSummary  `json:"summary"`
}

// BenchmarkSummary contains aggregate performance metrics
type BenchmarkSummary struct {
	TotalTests          int           `json:"total_tests"`
	SuccessfulTests     int           `json:"successful_tests"`
	FailedTests         int           `json:"failed_tests"`
	PostgreSQLAvgTime   time.Duration `json:"postgresql_avg_time"`
	ClickHouseAvgTime   time.Duration `json:"clickhouse_avg_time"`
	PerformanceGain     float64       `json:"performance_gain"`
	RecommendedDatabase string        `json:"recommended_database"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--help" {
		printHelp()
		return
	}

	fmt.Println("State Expiry Indexer Archive System Benchmark")
	fmt.Println("==============================================")
	fmt.Println()

	// Load configuration
	config, err := internal.LoadConfig("../configs")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	suite := BenchmarkSuite{
		Timestamp:   time.Now(),
		Environment: config.Environment,
		Results:     []BenchmarkResult{},
	}

	// Run benchmarks
	ctx := context.Background()
	runPostgreSQLBenchmarks(ctx, config, &suite)
	runClickHouseBenchmarks(ctx, config, &suite)

	// Calculate summary
	calculateSummary(&suite)

	// Output results
	outputResults(suite)

	fmt.Println("\nBenchmark completed successfully!")
	fmt.Printf("Results saved to: archive_benchmark_results_%s.json\n",
		time.Now().Format("20060102_150405"))
}

func runPostgreSQLBenchmarks(ctx context.Context, config internal.Config, suite *BenchmarkSuite) {
	fmt.Println("Running PostgreSQL benchmarks...")

	// Setup PostgreSQL connection
	config.ArchiveMode = false
	db, err := database.Connect(ctx, config)
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL: %v", err)
		return
	}
	defer db.Close()

	repo := repository.NewPostgreSQLRepository(db)

	// Define test cases
	testCases := []struct {
		name        string
		expiryBlock uint64
	}{
		{"Small_Dataset_1M", 1000000},
		{"Medium_Dataset_5M", 5000000},
		{"Large_Dataset_10M", 10000000},
		{"Very_Large_Dataset_20M", 20000000},
	}

	for _, tc := range testCases {
		fmt.Printf("  Running PostgreSQL test: %s...", tc.name)

		start := time.Now()
		_, err := repo.GetAnalyticsData(ctx, tc.expiryBlock, tc.expiryBlock+100000)
		duration := time.Since(start)

		result := BenchmarkResult{
			TestName:    tc.name,
			Database:    "PostgreSQL",
			ExpiryBlock: tc.expiryBlock,
			Duration:    duration,
			Success:     err == nil,
		}

		if err != nil {
			result.ErrorMessage = err.Error()
		}

		suite.Results = append(suite.Results, result)

		if err == nil {
			fmt.Printf(" ✓ %v\n", duration)
		} else {
			fmt.Printf(" ✗ %v\n", err)
		}
	}

	fmt.Println()
}

func runClickHouseBenchmarks(ctx context.Context, config internal.Config, suite *BenchmarkSuite) {
	fmt.Println("Running ClickHouse benchmarks...")

	// Setup ClickHouse connection
	config.ArchiveMode = true
	db, err := database.ConnectClickHouseSQL(config)
	if err != nil {
		log.Printf("Failed to connect to ClickHouse: %v", err)
		return
	}
	defer db.Close()

	repo := repository.NewClickHouseRepository(db)

	// Define test cases (same as PostgreSQL)
	testCases := []struct {
		name        string
		expiryBlock uint64
	}{
		{"Small_Dataset_1M", 1000000},
		{"Medium_Dataset_5M", 5000000},
		{"Large_Dataset_10M", 10000000},
		{"Very_Large_Dataset_20M", 20000000},
	}

	for _, tc := range testCases {
		fmt.Printf("  Running ClickHouse test: %s...", tc.name)

		start := time.Now()
		_, err := repo.GetAnalyticsData(ctx, tc.expiryBlock, tc.expiryBlock+100000)
		duration := time.Since(start)

		result := BenchmarkResult{
			TestName:    tc.name,
			Database:    "ClickHouse",
			ExpiryBlock: tc.expiryBlock,
			Duration:    duration,
			Success:     err == nil,
		}

		if err != nil {
			result.ErrorMessage = err.Error()
		}

		suite.Results = append(suite.Results, result)

		if err == nil {
			fmt.Printf(" ✓ %v\n", duration)
		} else {
			fmt.Printf(" ✗ %v\n", err)
		}
	}

	fmt.Println()
}

func calculateSummary(suite *BenchmarkSuite) {
	var pgTotalTime, chTotalTime time.Duration
	var pgCount, chCount int
	successfulTests := 0

	for _, result := range suite.Results {
		if result.Success {
			successfulTests++
			if result.Database == "PostgreSQL" {
				pgTotalTime += result.Duration
				pgCount++
			} else if result.Database == "ClickHouse" {
				chTotalTime += result.Duration
				chCount++
			}
		}
	}

	suite.Summary = BenchmarkSummary{
		TotalTests:      len(suite.Results),
		SuccessfulTests: successfulTests,
		FailedTests:     len(suite.Results) - successfulTests,
	}

	if pgCount > 0 {
		suite.Summary.PostgreSQLAvgTime = pgTotalTime / time.Duration(pgCount)
	}

	if chCount > 0 {
		suite.Summary.ClickHouseAvgTime = chTotalTime / time.Duration(chCount)
	}

	// Calculate performance gain
	if pgCount > 0 && chCount > 0 && suite.Summary.ClickHouseAvgTime > 0 {
		ratio := float64(suite.Summary.PostgreSQLAvgTime) / float64(suite.Summary.ClickHouseAvgTime)
		suite.Summary.PerformanceGain = ratio

		if ratio > 1.5 {
			suite.Summary.RecommendedDatabase = "ClickHouse (Archive Mode)"
		} else if ratio < 0.8 {
			suite.Summary.RecommendedDatabase = "PostgreSQL (Default Mode)"
		} else {
			suite.Summary.RecommendedDatabase = "Both databases perform similarly"
		}
	}
}

func outputResults(suite BenchmarkSuite) {
	fmt.Println("Benchmark Results Summary")
	fmt.Println("=========================")
	fmt.Printf("Environment: %s\n", suite.Environment)
	fmt.Printf("Total Tests: %d\n", suite.Summary.TotalTests)
	fmt.Printf("Successful: %d\n", suite.Summary.SuccessfulTests)
	fmt.Printf("Failed: %d\n", suite.Summary.FailedTests)
	fmt.Println()

	if suite.Summary.PostgreSQLAvgTime > 0 {
		fmt.Printf("PostgreSQL Average Time: %v\n", suite.Summary.PostgreSQLAvgTime)
	}

	if suite.Summary.ClickHouseAvgTime > 0 {
		fmt.Printf("ClickHouse Average Time: %v\n", suite.Summary.ClickHouseAvgTime)
	}

	if suite.Summary.PerformanceGain > 0 {
		fmt.Printf("Performance Gain: %.2fx\n", suite.Summary.PerformanceGain)
		fmt.Printf("Recommendation: %s\n", suite.Summary.RecommendedDatabase)
	}

	fmt.Println()
	fmt.Println("Detailed Results:")
	fmt.Println("================")

	// Group results by test name for comparison
	resultMap := make(map[string][]BenchmarkResult)
	for _, result := range suite.Results {
		resultMap[result.TestName] = append(resultMap[result.TestName], result)
	}

	for testName, results := range resultMap {
		fmt.Printf("\n%s:\n", testName)
		for _, result := range results {
			status := "✓"
			if !result.Success {
				status = "✗"
			}
			fmt.Printf("  %s %-12s: %v", status, result.Database, result.Duration)
			if !result.Success {
				fmt.Printf(" (Error: %s)", result.ErrorMessage)
			}
			fmt.Println()
		}

		// Calculate relative performance for this test
		if len(results) == 2 && results[0].Success && results[1].Success {
			var pg, ch BenchmarkResult
			for _, r := range results {
				if r.Database == "PostgreSQL" {
					pg = r
				} else {
					ch = r
				}
			}

			if pg.Duration > 0 && ch.Duration > 0 {
				ratio := float64(pg.Duration) / float64(ch.Duration)
				if ratio > 1.1 {
					fmt.Printf("  → ClickHouse is %.1fx faster\n", ratio)
				} else if ratio < 0.9 {
					fmt.Printf("  → PostgreSQL is %.1fx faster\n", 1/ratio)
				} else {
					fmt.Printf("  → Similar performance\n")
				}
			}
		}
	}

	// Save detailed results to JSON file
	filename := fmt.Sprintf("archive_benchmark_results_%s.json",
		time.Now().Format("20060102_150405"))

	jsonData, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal results to JSON: %v", err)
		return
	}

	err = os.WriteFile(filename, jsonData, 0o644)
	if err != nil {
		log.Printf("Failed to write results to file: %v", err)
		return
	}
}

func printHelp() {
	fmt.Println("Archive System Benchmark Tool")
	fmt.Println("=============================")
	fmt.Println()
	fmt.Println("This tool benchmarks the performance of PostgreSQL vs ClickHouse")
	fmt.Println("for analytics queries in the state expiry indexer.")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  go run scripts/archive_benchmark.go")
	fmt.Println()
	fmt.Println("Prerequisites:")
	fmt.Println("  - PostgreSQL database with test data")
	fmt.Println("  - ClickHouse database with test data")
	fmt.Println("  - Proper configuration in configs/config.env")
	fmt.Println()
	fmt.Println("Output:")
	fmt.Println("  - Console output with performance comparison")
	fmt.Println("  - JSON file with detailed benchmark results")
	fmt.Println()
	fmt.Println("Test Cases:")
	fmt.Println("  - Small Dataset (1M blocks)")
	fmt.Println("  - Medium Dataset (5M blocks)")
	fmt.Println("  - Large Dataset (10M blocks)")
	fmt.Println("  - Very Large Dataset (20M blocks)")
}
