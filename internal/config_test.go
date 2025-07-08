package internal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidation(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		// Test with empty config
		config := Config{}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RPC_URLS")

		// Test with missing DB fields (non-archive mode)
		config.RPCURLS = []string{"http://localhost:8545"}
		err = validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DB_HOST")

		// Test with all required fields
		config.DBHost = "localhost"
		config.DBPort = "5432"
		config.DBUser = "user"
		config.DBPassword = "password"
		config.DBName = "test"
		config.DBMaxConns = 10
		config.DBMinConns = 2
		config.APIPort = 8080
		config.RPCTimeout = 30
		config.BlockBatchSize = 100
		config.PollInterval = 10
		config.RangeSize = 1000
		config.LogLevel = "info"
		config.LogFormat = "text"
		config.Environment = "development"
		err = validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("validates ranges and intervals", func(t *testing.T) {
		config := Config{
			RPCURLS:        []string{"http://localhost:8545"},
			DBHost:         "localhost",
			DBPort:         "5432",
			DBUser:         "user",
			DBPassword:     "password",
			DBName:         "test",
			DBMaxConns:     10,
			DBMinConns:     2,
			APIPort:        8080,
			RPCTimeout:     30,
			BlockBatchSize: 100,
			LogLevel:       "info",
			LogFormat:      "text",
			Environment:    "development",
		}

		// Test invalid range size
		config.RangeSize = 0
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RANGE_SIZE")

		// Test invalid poll interval
		config.RangeSize = 1000
		config.PollInterval = 0
		err = validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "POLL_INTERVAL_SECONDS")

		// Test valid ranges
		config.PollInterval = 10
		err = validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("validates archive mode requirements", func(t *testing.T) {
		config := Config{
			RPCURLS:        []string{"http://localhost:8545"},
			ArchiveMode:    true,
			APIPort:        8080,
			RPCTimeout:     30,
			BlockBatchSize: 100,
			PollInterval:   10,
			RangeSize:      1000,
			LogLevel:       "info",
			LogFormat:      "text",
			Environment:    "development",
		}

		// Test missing ClickHouse fields
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CLICKHOUSE_HOST")

		// Test with ClickHouse fields
		config.ClickHouseHost = "localhost"
		config.ClickHousePort = "9000"
		config.ClickHouseUser = "user"
		config.ClickHousePassword = "password"
		config.ClickHouseDatabase = "test"
		config.ClickHouseMaxConns = 10
		config.ClickHouseMinConns = 2
		err = validateConfig(config)
		assert.NoError(t, err)
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("loads config from environment variables", func(t *testing.T) {
		// Set environment variables
		os.Setenv("RPC_URLS", "http://test.example.com:8545")
		os.Setenv("DB_HOST", "testhost")
		os.Setenv("DB_PORT", "5433")
		os.Setenv("DB_USER", "testuser")
		os.Setenv("DB_PASSWORD", "testpass")
		os.Setenv("DB_NAME", "testdb")
		os.Setenv("RANGE_SIZE", "2000")
		os.Setenv("POLL_INTERVAL_SECONDS", "30")
		os.Setenv("DATA_DIR", "/tmp/testdata")
		os.Setenv("API_PORT", "8081")
		os.Setenv("ARCHIVE_MODE", "true")
		os.Setenv("CLICKHOUSE_HOST", "clickhouse-host")
		os.Setenv("CLICKHOUSE_PORT", "9000")
		os.Setenv("CLICKHOUSE_USER", "ch_user")
		os.Setenv("CLICKHOUSE_PASSWORD", "ch_pass")
		os.Setenv("CLICKHOUSE_DATABASE", "ch_db")
		defer func() {
			os.Unsetenv("RPC_URLS")
			os.Unsetenv("DB_HOST")
			os.Unsetenv("DB_PORT")
			os.Unsetenv("DB_USER")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("DB_NAME")
			os.Unsetenv("RANGE_SIZE")
			os.Unsetenv("POLL_INTERVAL_SECONDS")
			os.Unsetenv("DATA_DIR")
			os.Unsetenv("API_PORT")
			os.Unsetenv("ARCHIVE_MODE")
			os.Unsetenv("CLICKHOUSE_HOST")
			os.Unsetenv("CLICKHOUSE_PORT")
			os.Unsetenv("CLICKHOUSE_USER")
			os.Unsetenv("CLICKHOUSE_PASSWORD")
			os.Unsetenv("CLICKHOUSE_DATABASE")
		}()

		config, err := LoadConfig("./configs")
		assert.NoError(t, err)
		assert.Equal(t, []string{"http://test.example.com:8545"}, config.RPCURLS)
		assert.Equal(t, "testhost", config.DBHost)
		assert.Equal(t, "5433", config.DBPort)
		assert.Equal(t, "testuser", config.DBUser)
		assert.Equal(t, "testpass", config.DBPassword)
		assert.Equal(t, "testdb", config.DBName)
		assert.Equal(t, 2000, config.RangeSize)
		assert.Equal(t, 30, config.PollInterval)
		assert.Equal(t, 8081, config.APIPort)
		assert.True(t, config.ArchiveMode)
		assert.Equal(t, "clickhouse-host", config.ClickHouseHost)
	})

	t.Run("uses default values for optional fields", func(t *testing.T) {
		// Set only required fields
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

		config, err := LoadConfig("./configs")
		assert.NoError(t, err)
		
		// Check default values
		assert.Equal(t, 1000, config.RangeSize)
		assert.Equal(t, 10, config.PollInterval)
		assert.Equal(t, 8080, config.APIPort)
		assert.False(t, config.ArchiveMode)
	})

	t.Run("handles invalid environment variables", func(t *testing.T) {
		// Set invalid port
		os.Setenv("RPC_URLS", "http://localhost:8545")
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "invalid")
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

		_, err := LoadConfig("./configs")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DB_PORT")
	})

	t.Run("handles missing required environment variables", func(t *testing.T) {
		// Don't set any environment variables but clear existing ones
		originalRPC := os.Getenv("RPC_URLS")
		os.Unsetenv("RPC_URLS")
		defer func() {
			if originalRPC != "" {
				os.Setenv("RPC_URLS", originalRPC)
			}
		}()

		_, err := LoadConfig("./configs")
		assert.Error(t, err)
	})
}

func TestConfigMethods(t *testing.T) {
	t.Run("creates database connection string", func(t *testing.T) {
		config := Config{
			DBHost:     "localhost",
			DBPort:     "5432",
			DBUser:     "testuser",
			DBPassword: "testpass",
			DBName:     "testdb",
		}

		connStr := config.GetDatabaseConnectionString()
		expected := "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable"
		assert.Equal(t, expected, connStr)
	})

	t.Run("creates ClickHouse connection string", func(t *testing.T) {
		config := Config{
			ClickHouseHost:     "localhost",
			ClickHousePort:     "9000",
			ClickHouseUser:     "testuser",
			ClickHousePassword: "testpass",
			ClickHouseDatabase: "testdb",
		}

		connStr := config.GetClickHouseConnectionString(false)
		expected := "clickhouse://testuser:testpass@localhost:9000/testdb?secure=false"
		assert.Equal(t, expected, connStr)
	})

	t.Run("environment checks work correctly", func(t *testing.T) {
		config := Config{Environment: "production"}
		assert.True(t, config.IsProduction())
		assert.False(t, config.IsDevelopment())

		config.Environment = "development"
		assert.False(t, config.IsProduction())
		assert.True(t, config.IsDevelopment())
	})
}