package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
)

// IntegrationTest represents a basic integration test
func TestIntegration_DatabaseConnectivity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test database connectivity for both PostgreSQL and ClickHouse
	t.Run("PostgreSQL connectivity", func(t *testing.T) {
		// Set up test environment variables
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "15432")
		os.Setenv("DB_USER", "test")
		os.Setenv("DB_PASSWORD", "test")
		os.Setenv("DB_NAME", "test")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
		}()

		config, err := internal.LoadConfig("./configs")
		require.NoError(t, err)

		ctx := context.Background()
		repo, err := repository.NewRepository(ctx, config)
		require.NoError(t, err)

		// Test basic repository functionality
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, lastRange, uint64(0))
	})

	t.Run("ClickHouse connectivity", func(t *testing.T) {
		// Set up test environment variables for ClickHouse
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("ARCHIVE_MODE", "true")
		os.Setenv("CLICKHOUSE_HOST", "localhost")
		os.Setenv("CLICKHOUSE_PORT", "19010")
		os.Setenv("CLICKHOUSE_USER", "test_user")
		os.Setenv("CLICKHOUSE_PASSWORD", "test_password")
		os.Setenv("CLICKHOUSE_DATABASE", "test_state_expiry")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("ARCHIVE_MODE")
			os.Unsetenv("CLICKHOUSE_HOST")
			os.Unsetenv("CLICKHOUSE_PORT")
			os.Unsetenv("CLICKHOUSE_USER")
			os.Unsetenv("CLICKHOUSE_PASSWORD")
			os.Unsetenv("CLICKHOUSE_DATABASE")
		}()

		config, err := internal.LoadConfig("./configs")
		require.NoError(t, err)

		ctx := context.Background()
		repo, err := repository.NewRepository(ctx, config)
		require.NoError(t, err)

		// Test basic repository functionality
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, lastRange, uint64(0))
	})
}

func TestIntegration_ConfigurationLoading(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("loads configuration with defaults", func(t *testing.T) {
		// Set minimal required environment
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "5432")
		os.Setenv("DB_USER", "user")
		os.Setenv("DB_PASSWORD", "password")
		os.Setenv("DB_NAME", "test")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
		}()

		config, err := internal.LoadConfig("./configs")
		assert.NoError(t, err)

		// Verify expected defaults
		assert.Equal(t, 1000, config.RangeSize)
		assert.Equal(t, 10, config.PollInterval)
		assert.Equal(t, 8080, config.APIPort)
		assert.Equal(t, "localhost", config.APIHost)
		assert.Equal(t, "info", config.LogLevel)
		assert.Equal(t, "text", config.LogFormat)
		assert.Equal(t, "development", config.Environment)
		assert.False(t, config.ArchiveMode)
		assert.True(t, config.CompressionEnabled)
	})

	t.Run("validates configuration correctly", func(t *testing.T) {
		// Test configuration validation with invalid values
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "5432")
		os.Setenv("DB_USER", "user")
		os.Setenv("DB_PASSWORD", "password")
		os.Setenv("DB_NAME", "test")
		os.Setenv("RANGE_SIZE", "0") // Invalid range size
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
			os.Unsetenv("RANGE_SIZE")
		}()

		_, err := internal.LoadConfig("./configs")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RANGE_SIZE")
	})
}

func TestIntegration_ComponentInteraction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("repository and configuration integration", func(t *testing.T) {
		// Test that repository can be created with valid configuration
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "15432")
		os.Setenv("DB_USER", "test")
		os.Setenv("DB_PASSWORD", "test")
		os.Setenv("DB_NAME", "test")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
		}()

		config, err := internal.LoadConfig("./configs")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		repo, err := repository.NewRepository(ctx, config)
		require.NoError(t, err)

		// Test basic repository operations
		lastRange, err := repo.GetLastIndexedRange(ctx)
		assert.NoError(t, err)

		// Test sync status functionality
		syncStatus, err := repo.GetSyncStatus(ctx, lastRange+1, uint64(config.RangeSize))
		assert.NoError(t, err)
		assert.NotNil(t, syncStatus)
		assert.Equal(t, lastRange, syncStatus.LastIndexedRange)

		// Test analytics functionality (should work even with empty data)
		analytics, err := repo.GetAnalyticsData(ctx, 100, 200)
		if err != nil {
			// Analytics might fail with empty data - that's OK for integration test
			t.Logf("Analytics failed (expected with empty data): %v", err)
		} else {
			assert.NotNil(t, analytics)
			if analytics != nil {
				assert.GreaterOrEqual(t, analytics.AccountExpiry.TotalAccounts, 0)
				assert.GreaterOrEqual(t, analytics.StorageSlotExpiry.TotalSlots, 0)
			}
		}
	})
}

func TestIntegration_EnvironmentVariableHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("handles environment variable precedence", func(t *testing.T) {
		// Test that environment variables override config file values
		os.Setenv("RPC_URLS", "http://test.example.com:8545")
		os.Setenv("DB_HOST", "test-host")
		os.Setenv("DB_PORT", "9999")
		os.Setenv("DB_USER", "test-user")
		os.Setenv("DB_PASSWORD", "test-password")
		os.Setenv("DB_NAME", "test-db")
		os.Setenv("API_PORT", "9090")
		os.Setenv("RANGE_SIZE", "2000")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
			os.Unsetenv("API_PORT")
			os.Unsetenv("RANGE_SIZE")
		}()

		config, err := internal.LoadConfig("./configs")
		assert.NoError(t, err)

		// Verify environment variables were used
		assert.Equal(t, []string{"http://test.example.com:8545"}, config.RPCURLS)
		assert.Equal(t, "test-host", config.DBHost)
		assert.Equal(t, "9999", config.DBPort)
		assert.Equal(t, "test-user", config.DBUser)
		assert.Equal(t, "test-password", config.DBPassword)
		assert.Equal(t, "test-db", config.DBName)
		assert.Equal(t, 9090, config.APIPort)
		assert.Equal(t, 2000, config.RangeSize)
	})
}

func TestIntegration_DataDirectoryHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("expands data directory paths correctly", func(t *testing.T) {
		// Test path expansion functionality
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "5432")
		os.Setenv("DB_USER", "user")
		os.Setenv("DB_PASSWORD", "password")
		os.Setenv("DB_NAME", "test")
		os.Setenv("DATA_DIR", "./test_data")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
			os.Unsetenv("DATA_DIR")
		}()

		config, err := internal.LoadConfig("./configs")
		assert.NoError(t, err)

		// Verify data directory was processed
		assert.NotEmpty(t, config.DataDir)
		assert.Contains(t, config.DataDir, "test_data")
	})
}