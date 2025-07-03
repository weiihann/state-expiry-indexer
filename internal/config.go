package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	// Database configuration
	DBHost     string `mapstructure:"DB_HOST"`
	DBPort     string `mapstructure:"DB_PORT"`
	DBUser     string `mapstructure:"DB_USER"`
	DBPassword string `mapstructure:"DB_PASSWORD"`
	DBName     string `mapstructure:"DB_NAME"`
	DBMaxConns int    `mapstructure:"DB_MAX_CONNS"`
	DBMinConns int    `mapstructure:"DB_MIN_CONNS"`

	// RPC configuration
	RPCURLS    []string `mapstructure:"RPC_URLS"`
	RPCTimeout int      `mapstructure:"RPC_TIMEOUT_SECONDS"`

	// API Server configuration
	APIPort int    `mapstructure:"API_PORT"`
	APIHost string `mapstructure:"API_HOST"`

	// File storage configuration
	DataDir string `mapstructure:"DATA_DIR"`

	// Indexer configuration
	BlockBatchSize int `mapstructure:"BLOCK_BATCH_SIZE"`
	PollInterval   int `mapstructure:"POLL_INTERVAL_SECONDS"`

	// Logging configuration
	LogLevel  string `mapstructure:"LOG_LEVEL"`
	LogFormat string `mapstructure:"LOG_FORMAT"`
	LogFile   string `mapstructure:"LOG_FILE"`

	// Runtime environment
	Environment string `mapstructure:"ENVIRONMENT"`
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

	// RPC defaults
	viper.SetDefault("RPC_URL", "")
	viper.SetDefault("RPC_TIMEOUT_SECONDS", 30)

	// API Server defaults
	viper.SetDefault("API_PORT", 8080)
	viper.SetDefault("API_HOST", "localhost")

	// File storage defaults
	viper.SetDefault("DATA_DIR", "data")
	viper.SetDefault("STATE_DIFF_DIR", "data/statediffs")

	// Indexer defaults
	viper.SetDefault("BLOCK_BATCH_SIZE", 100)
	viper.SetDefault("POLL_INTERVAL_SECONDS", 10)

	// Logging defaults
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("LOG_FORMAT", "text")
	viper.SetDefault("LOG_FILE", "")

	// Runtime defaults
	viper.SetDefault("ENVIRONMENT", "development")
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

	if config.DBHost == "" {
		errors = append(errors, ValidationError{
			Field:   "DB_HOST",
			Message: "database host is required",
		})
	}

	if config.DBName == "" {
		errors = append(errors, ValidationError{
			Field:   "DB_NAME",
			Message: "database name is required",
		})
	}

	if config.DBUser == "" {
		errors = append(errors, ValidationError{
			Field:   "DB_USER",
			Message: "database user is required",
		})
	}

	// Port validation
	if config.DBPort == "" {
		errors = append(errors, ValidationError{
			Field:   "DB_PORT",
			Message: "database port is required",
		})
	} else if port, err := strconv.Atoi(config.DBPort); err != nil || port <= 0 || port > 65535 {
		errors = append(errors, ValidationError{
			Field:   "DB_PORT",
			Message: "database port must be a valid port number (1-65535)",
		})
	}

	if config.APIPort <= 0 || config.APIPort > 65535 {
		errors = append(errors, ValidationError{
			Field:   "API_PORT",
			Message: "API port must be a valid port number (1-65535)",
		})
	}

	// Connection pool validation
	if config.DBMaxConns <= 0 {
		errors = append(errors, ValidationError{
			Field:   "DB_MAX_CONNS",
			Message: "database max connections must be greater than 0",
		})
	}

	if config.DBMinConns < 0 {
		errors = append(errors, ValidationError{
			Field:   "DB_MIN_CONNS",
			Message: "database min connections must be greater than or equal to 0",
		})
	}

	if config.DBMinConns > config.DBMaxConns {
		errors = append(errors, ValidationError{
			Field:   "DB_MIN_CONNS",
			Message: "database min connections cannot be greater than max connections",
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

// GetDatabaseConnectionString builds a PostgreSQL connection string
func (c *Config) GetDatabaseConnectionString() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.DBUser,
		c.DBPassword,
		c.DBHost,
		c.DBPort,
		c.DBName,
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
