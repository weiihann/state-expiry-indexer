package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"

	// Database drivers
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

// TestConfig represents configuration for test databases
type TestConfig struct {
	PostgreSQL TestDBConfig
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
		PostgreSQL: TestDBConfig{
			Host:     "localhost",
			Port:     "15432",
			User:     "test",
			Password: "test",
			Database: "test",
		},
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
func SetupTestDatabase(t *testing.T, archiveMode bool) func() {
	t.Helper()

	testConfig := GetTestConfig()

	if archiveMode {
		return setupClickHouseTestDB(t, testConfig.ClickHouse)
	}
	return setupPostgreSQLTestDB(t, testConfig.PostgreSQL)
}

// setupPostgreSQLTestDB sets up a PostgreSQL test database
func setupPostgreSQLTestDB(t *testing.T, dbConfig TestDBConfig) func() {
	t.Helper()

	// Create test configuration
	config := internal.Config{
		DBHost:      dbConfig.Host,
		DBPort:      dbConfig.Port,
		DBUser:      dbConfig.User,
		DBPassword:  dbConfig.Password,
		DBName:      dbConfig.Database,
		DBMaxConns:  10,
		DBMinConns:  2,
		RPCURLS:     []string{"http://localhost:8545"}, // Required for config validation
		Environment: "test",
		LogLevel:    "error", // Reduce log noise in tests
		ArchiveMode: false,
	}

	// Wait for database to be ready
	WaitForPostgreSQL(t, config, 30*time.Second)

	// Create SQL connection for migrations
	sqlDB, err := sql.Open("postgres", config.GetDatabaseConnectionString())
	require.NoError(t, err, "failed to create SQL connection")

	// Run migrations
	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	require.NoError(t, err, "failed to create postgres driver")

	m, err := migrate.NewWithDatabaseInstance(
		"file://../../db/migrations", // Relative path from test location
		"postgres", driver)
	require.NoError(t, err, "failed to create migrate instance")

	// Apply migrations
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply migrations")
	}

	// Close migration resources
	sourceErr, dbErr := m.Close()
	require.NoError(t, sourceErr, "failed to close migration source")
	require.NoError(t, dbErr, "failed to close migration database")

	err = sqlDB.Close()
	require.NoError(t, err, "failed to close SQL connection")

	// Return cleanup function
	cleanup := func() {
		cleanupPostgreSQLTestDB(t, config)
	}

	return cleanup
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
		ArchiveMode:        true,
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
		"file://../../db/ch-migrations",
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

// waitForPostgreSQL waits for PostgreSQL to be ready
func WaitForPostgreSQL(t *testing.T, config internal.Config, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for PostgreSQL to be ready")
		case <-ticker.C:
			db, err := pgxpool.Connect(ctx, config.GetDatabaseConnectionString())
			if err == nil {
				err = db.Ping(ctx)
				if err == nil {
					db.Close()
					return
				}
				db.Close()
			}
		}
	}
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

// cleanupPostgreSQLTestDB cleans up PostgreSQL test database by dropping all tables
func cleanupPostgreSQLTestDB(t *testing.T, config internal.Config) {
	t.Helper()

	ctx := context.Background()
	db, err := pgxpool.Connect(ctx, config.GetDatabaseConnectionString())
	if err != nil {
		t.Logf("failed to connect for cleanup: %v", err)
		return
	}
	defer db.Close()

	// Drop all tables to clean up
	_, err = db.Exec(ctx, `
		DROP TABLE IF EXISTS accounts_current CASCADE;
		DROP TABLE IF EXISTS storage_current CASCADE;
		DROP TABLE IF EXISTS metadata CASCADE;
		DROP SCHEMA IF EXISTS public CASCADE;
		CREATE SCHEMA public;
		GRANT ALL ON SCHEMA public TO PUBLIC;
	`)
	if err != nil {
		t.Logf("failed to cleanup PostgreSQL test database: %v", err)
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

	for rows.Next() {
		var dbName, tableName string
		if err := rows.Scan(&dbName, &tableName); err != nil {
			t.Logf("failed to scan table info: %v", err)
			continue
		}

		stmt := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tableName)
		if _, err := db.Exec(stmt); err != nil {
			t.Logf("failed to drop table %s.%s: %v", dbName, tableName, err)
		}
	}
}
