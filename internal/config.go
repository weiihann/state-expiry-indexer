package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// TODO:
type Config struct {
	// ClickHouse Archive configuration
	ClickHouseHost     string `mapstructure:"CLICKHOUSE_HOST"`
	ClickHousePort     string `mapstructure:"CLICKHOUSE_PORT"`
	ClickHouseUser     string `mapstructure:"CLICKHOUSE_USER"`
	ClickHousePassword string `mapstructure:"CLICKHOUSE_PASSWORD"`
	ClickHouseDatabase string `mapstructure:"CLICKHOUSE_DATABASE"`
	ClickHouseMaxConns int    `mapstructure:"CLICKHOUSE_MAX_CONNS"`
	ClickHouseMinConns int    `mapstructure:"CLICKHOUSE_MIN_CONNS"`

	// RPC configuration
	RPCURLS    []string `mapstructure:"RPC_URLS"`
	RPCTimeout int      `mapstructure:"RPC_TIMEOUT_SECONDS"`

	// API Server configuration
	APIPort int    `mapstructure:"API_PORT"`
	APIHost string `mapstructure:"API_HOST"`

	// Prometheus metrics server configuration
	PrometheusHost string `mapstructure:"PROMETHEUS_HOST"`
	PrometheusPort int    `mapstructure:"PROMETHEUS_PORT"`

	// File storage configuration
	DataDir string `mapstructure:"DATA_DIR"`

	// Indexer configuration
	BlockBatchSize int `mapstructure:"BLOCK_BATCH_SIZE"`
	PollInterval   int `mapstructure:"POLL_INTERVAL_SECONDS"`
	RangeSize      int `mapstructure:"RANGE_SIZE"`

	// Logging configuration
	LogLevel  string `mapstructure:"LOG_LEVEL"`
	LogFormat string `mapstructure:"LOG_FORMAT"`
	LogFile   string `mapstructure:"LOG_FILE"`

	// Runtime environment
	Environment string `mapstructure:"ENVIRONMENT"`

	// Compression configuration
	CompressionEnabled bool `mapstructure:"COMPRESSION_ENABLED"`
}

// ValidationError represents configuration validation errors
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("config validation error for field '%s': %s", e.Field, e.Message)
}

// ValidationErrors represents multiple validation errors
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("configuration validation failed:\n%s", strings.Join(messages, "\n"))
}

func LoadConfig(path string) (config Config, err error) {
	// Configure viper
	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	// Set comprehensive defaults
	setDefaults()

	// Try to read config file
	if err := viper.ReadInConfig(); err != nil {
		// Check if it's a file not found error
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found, but that's okay - we can use environment variables and defaults
		} else {
			// Config file was found but another error was produced
			return config, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Unmarshal into config struct
	if err := viper.Unmarshal(&config); err != nil {
		return config, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return config, err
	}

	// Expand data directory paths
	config.DataDir = expandPath(config.DataDir)
	if config.LogFile != "" {
		config.LogFile = expandPath(config.LogFile)
	}

	return config, nil
}

func setDefaults() {
	// Database defaults
	viper.SetDefault("DB_HOST", "localhost")
	viper.SetDefault("DB_PORT", "5432")
	viper.SetDefault("DB_USER", "user")
	viper.SetDefault("DB_PASSWORD", "password")
	viper.SetDefault("DB_NAME", "state_expiry")
	viper.SetDefault("DB_MAX_CONNS", 10)
	viper.SetDefault("DB_MIN_CONNS", 2)

	// ClickHouse defaults
	viper.SetDefault("ARCHIVE_MODE", false)
	viper.SetDefault("CLICKHOUSE_HOST", "localhost")
	viper.SetDefault("CLICKHOUSE_PORT", "9010")
	viper.SetDefault("CLICKHOUSE_USER", "user")
	viper.SetDefault("CLICKHOUSE_PASSWORD", "password")
	viper.SetDefault("CLICKHOUSE_DATABASE", "state_expiry")
	viper.SetDefault("CLICKHOUSE_MAX_CONNS", 10)
	viper.SetDefault("CLICKHOUSE_MIN_CONNS", 2)

	// RPC defaults
	viper.SetDefault("RPC_URL", "")
	viper.SetDefault("RPC_TIMEOUT_SECONDS", 30)

	// API Server defaults
	viper.SetDefault("API_PORT", 8080)
	viper.SetDefault("API_HOST", "localhost")

	// Prometheus metrics server defaults
	viper.SetDefault("PROMETHEUS_HOST", "localhost")
	viper.SetDefault("PROMETHEUS_PORT", 9000)

	// File storage defaults
	viper.SetDefault("DATA_DIR", "data")
	viper.SetDefault("STATE_DIFF_DIR", "data/statediffs")

	// Indexer defaults
	viper.SetDefault("BLOCK_BATCH_SIZE", 100)
	viper.SetDefault("POLL_INTERVAL_SECONDS", 60)
	viper.SetDefault("RANGE_SIZE", 1000)

	// Logging defaults
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("LOG_FORMAT", "text")
	viper.SetDefault("LOG_FILE", "")

	// Runtime defaults
	viper.SetDefault("ENVIRONMENT", "development")

	// Compression defaults
	viper.SetDefault("COMPRESSION_ENABLED", true)
}

func validateConfig(config Config) error {
	var errors ValidationErrors

	// Required fields validation
	if len(config.RPCURLS) == 0 {
		errors = append(errors, ValidationError{
			Field:   "RPC_URLS",
			Message: "RPC URL is required for connecting to Ethereum node",
		})
	}

	// Validate database configuration based on archive mode
	// ClickHouse validation
	if config.ClickHouseHost == "" {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_HOST",
			Message: "ClickHouse host is required when archive mode is enabled",
		})
	}

	if config.ClickHouseDatabase == "" {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_DATABASE",
			Message: "ClickHouse database name is required when archive mode is enabled",
		})
	}

	if config.ClickHouseUser == "" {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_USER",
			Message: "ClickHouse user is required when archive mode is enabled",
		})
	}

	// Port validation based on archive mode
	// ClickHouse port validation
	if config.ClickHousePort == "" {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_PORT",
			Message: "ClickHouse port is required when archive mode is enabled",
		})
	} else if port, err := strconv.Atoi(config.ClickHousePort); err != nil || port <= 0 || port > 65535 {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_PORT",
			Message: "ClickHouse port must be a valid port number (1-65535)",
		})
	}

	if config.APIPort <= 0 || config.APIPort > 65535 {
		errors = append(errors, ValidationError{
			Field:   "API_PORT",
			Message: "API port must be a valid port number (1-65535)",
		})
	}

	if config.PrometheusHost == "" {
		errors = append(errors, ValidationError{
			Field:   "PROMETHEUS_HOST",
			Message: "Prometheus host is required",
		})
	}
	if config.PrometheusPort == 0 {
		errors = append(errors, ValidationError{
			Field:   "PROMETHEUS_PORT",
			Message: "Prometheus port is required",
		})
	}

	// Connection pool validation based on archive mode
	// ClickHouse connection pool validation
	if config.ClickHouseMaxConns <= 0 {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_MAX_CONNS",
			Message: "ClickHouse max connections must be greater than 0",
		})
	}

	if config.ClickHouseMinConns < 0 {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_MIN_CONNS",
			Message: "ClickHouse min connections must be greater than or equal to 0",
		})
	}

	if config.ClickHouseMinConns > config.ClickHouseMaxConns {
		errors = append(errors, ValidationError{
			Field:   "CLICKHOUSE_MIN_CONNS",
			Message: "ClickHouse min connections cannot be greater than max connections",
		})
	}

	// Timeout validation
	if config.RPCTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "RPC_TIMEOUT_SECONDS",
			Message: "RPC timeout must be greater than 0 seconds",
		})
	}

	// Batch size validation
	if config.BlockBatchSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   "BLOCK_BATCH_SIZE",
			Message: "block batch size must be greater than 0",
		})
	}

	// Poll interval validation
	if config.PollInterval <= 0 {
		errors = append(errors, ValidationError{
			Field:   "POLL_INTERVAL_SECONDS",
			Message: "poll interval must be greater than 0 seconds",
		})
	}

	// Range size validation
	if config.RangeSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   "RANGE_SIZE",
			Message: "range size must be greater than 0",
		})
	}

	// Log level validation
	validLogLevels := []string{"debug", "info", "warn", "error"}
	if !contains(validLogLevels, strings.ToLower(config.LogLevel)) {
		errors = append(errors, ValidationError{
			Field:   "LOG_LEVEL",
			Message: fmt.Sprintf("log level must be one of: %s", strings.Join(validLogLevels, ", ")),
		})
	}

	// Log format validation
	validLogFormats := []string{"text", "json"}
	if !contains(validLogFormats, strings.ToLower(config.LogFormat)) {
		errors = append(errors, ValidationError{
			Field:   "LOG_FORMAT",
			Message: fmt.Sprintf("log format must be one of: %s", strings.Join(validLogFormats, ", ")),
		})
	}

	// Environment validation
	validEnvironments := []string{"development", "staging", "production"}
	if !contains(validEnvironments, strings.ToLower(config.Environment)) {
		errors = append(errors, ValidationError{
			Field:   "ENVIRONMENT",
			Message: fmt.Sprintf("environment must be one of: %s", strings.Join(validEnvironments, ", ")),
		})
	}

	// Log file validation (if specified)
	if config.LogFile != "" {
		logDir := filepath.Dir(config.LogFile)
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			errors = append(errors, ValidationError{
				Field:   "LOG_FILE",
				Message: fmt.Sprintf("cannot create log file directory '%s': %v", logDir, err),
			})
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// expandPath expands ~ and environment variables in paths
func expandPath(path string) string {
	if path == "" {
		return path
	}

	// Expand environment variables
	path = os.ExpandEnv(path)

	// Expand home directory
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			path = filepath.Join(home, path[2:])
		}
	}

	return path
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// GetClickHouseConnectionString builds a ClickHouse connection string for golang-migrate
func (c *Config) GetClickHouseConnectionString(migrate bool) string {
	// golang-migrate ClickHouse driver expects clickhouse:// protocol for native TCP connection
	if migrate {
		return fmt.Sprintf(
			"clickhouse://%s:%s@%s:%s/%s?secure=false&x-multi-statement=true",
			c.ClickHouseUser,
			c.ClickHousePassword,
			c.ClickHouseHost,
			c.ClickHousePort,
			c.ClickHouseDatabase,
		)
	}
	return fmt.Sprintf(
		"clickhouse://%s:%s@%s:%s/%s?secure=false",
		c.ClickHouseUser,
		c.ClickHousePassword,
		c.ClickHouseHost,
		c.ClickHousePort,
		c.ClickHouseDatabase,
	)
}

// IsProduction returns true if running in production environment
func (c *Config) IsProduction() bool {
	return strings.ToLower(c.Environment) == "production"
}

// IsDevelopment returns true if running in development environment
func (c *Config) IsDevelopment() bool {
	return strings.ToLower(c.Environment) == "development"
}
