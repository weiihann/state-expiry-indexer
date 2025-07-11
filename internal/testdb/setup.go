package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"

	// Database drivers
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

// TestConfig represents configuration for test databases
type TestConfig struct {
	ClickHouse TestDBConfig
}

type TestDBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// GetTestConfig returns the test database configuration
func GetTestConfig() TestConfig {
	return TestConfig{
		ClickHouse: TestDBConfig{
			Host:     "localhost",
			Port:     "19010",
			User:     "test_user",
			Password: "test_password",
			Database: "test_state_expiry",
		},
	}
}

// SetupTestDatabase sets up a test database with migrations and returns a cleanup function
func SetupTestDatabase(t *testing.T) func() {
	t.Helper()

	testConfig := GetTestConfig()
	return setupClickHouseTestDB(t, testConfig.ClickHouse)
}

// setupClickHouseTestDB sets up a ClickHouse test database
func setupClickHouseTestDB(t *testing.T, dbConfig TestDBConfig) func() {
	t.Helper()

	// Create test configuration
	config := internal.Config{
		ClickHouseHost:     dbConfig.Host,
		ClickHousePort:     dbConfig.Port,
		ClickHouseUser:     dbConfig.User,
		ClickHousePassword: dbConfig.Password,
		ClickHouseDatabase: dbConfig.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"}, // Required for config validation
		Environment:        "test",
		LogLevel:           "error", // Reduce log noise in tests
	}

	// Wait for database to be ready
	WaitForClickHouse(t, config, 30*time.Second)

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	if err != nil {
		t.Fatalf("failed to open ClickHouse connection: %v", err)
	}
	defer d.Close()

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://../../db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	require.NoError(t, err, "failed to create ClickHouse migrate instance")

	// Apply migrations
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply ClickHouse migrations")
	}

	// Close migration resources
	sourceErr, dbErr := m.Close()
	require.NoError(t, sourceErr, "failed to close migration source")
	require.NoError(t, dbErr, "failed to close migration database")

	// Return cleanup function
	cleanup := func() {
		cleanupClickHouseTestDB(t, config)
	}

	return cleanup
}

// waitForClickHouse waits for ClickHouse to be ready
func WaitForClickHouse(t *testing.T, config internal.Config, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for ClickHouse to be ready")
		case <-ticker.C:
			db, err := sql.Open("clickhouse", config.GetClickHouseConnectionString(false))
			if err == nil {
				err = db.Ping()
				if err == nil {
					db.Close()
					return
				}
				db.Close()
			}
		}
	}
}

// cleanupClickHouseTestDB cleans up ClickHouse test database by dropping all tables
func cleanupClickHouseTestDB(t *testing.T, config internal.Config) {
	t.Helper()

	db, err := sql.Open("clickhouse", config.GetClickHouseConnectionString(false))
	if err != nil {
		t.Logf("failed to connect for cleanup: %v", err)
		return
	}
	defer db.Close()

	rows, err := db.Query(`
    SELECT database, name 
    FROM system.tables 
    WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
`)
	if err != nil {
		t.Logf("failed to list ClickHouse tables: %v", err)
		return
	}
	defer rows.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Logf("failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	for rows.Next() {
		var dbName, tableName string
		if err := rows.Scan(&dbName, &tableName); err != nil {
			t.Logf("failed to scan table info: %v", err)
			continue
		}

		stmt := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tableName)
		if _, err := tx.Exec(stmt); err != nil {
			t.Logf("failed to drop table %s.%s: %v", dbName, tableName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Logf("failed to commit transaction: %v", err)
	}
}
