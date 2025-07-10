package cmd

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/testdb"

	// Database drivers
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

// Test ClickHouse migration up operations
func TestClickHouseMigrateUp(t *testing.T) {
	testConfig := testdb.GetTestConfig()

	// Create test configuration for ClickHouse
	config := internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
		LogLevel:           "error",
	}

	// Wait for database to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get ClickHouse connection string and test connection
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	require.NoError(t, err, "failed to open ClickHouse connection")
	defer d.Close()

	// Test connection by creating SQL connection (without migration parameters)
	testConnectionString := config.GetClickHouseConnectionString(false)
	chTestDB, err := sql.Open("clickhouse", testConnectionString)
	require.NoError(t, err, "failed to create ClickHouse test connection")
	defer chTestDB.Close()

	// Wait for connection
	for {
		if err := chTestDB.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("ClickHouse not ready within timeout")
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://../db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	require.NoError(t, err, "failed to create ClickHouse migrate instance")
	defer m.Close()

	// Test migration up
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply ClickHouse migrations")
	}

	// Verify migrations were applied by checking version
	version, dirty, err := m.Version()
	require.NoError(t, err, "failed to get ClickHouse migration version")
	assert.False(t, dirty, "ClickHouse migration should not be in dirty state")
	assert.Greater(t, version, uint(0), "ClickHouse migration version should be greater than 0")

	// Verify tables exist by querying through the driver
	query := "SELECT count() FROM system.tables WHERE database = ? AND name = ?"

	// Create a new connection for queries (without migration-specific parameters)
	queryConnectionString := config.GetClickHouseConnectionString(false)
	chDB, err := sql.Open("clickhouse", queryConnectionString)
	require.NoError(t, err, "failed to create ClickHouse SQL connection")
	defer chDB.Close()

	var count int
	err = chDB.QueryRow(query, config.ClickHouseDatabase, "accounts_archive").Scan(&count)
	require.NoError(t, err, "failed to query accounts_archive table existence")
	assert.Equal(t, 1, count, "accounts_archive table should exist after migration")

	err = chDB.QueryRow(query, config.ClickHouseDatabase, "storage_archive").Scan(&count)
	require.NoError(t, err, "failed to query storage_archive table existence")
	assert.Equal(t, 1, count, "storage_archive table should exist after migration")

	err = chDB.QueryRow(query, config.ClickHouseDatabase, "metadata_archive").Scan(&count)
	require.NoError(t, err, "failed to query metadata_archive table existence")
	assert.Equal(t, 1, count, "metadata_archive table should exist after migration")

	// Clean up - force down all migrations
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		t.Logf("Warning: failed to clean up ClickHouse migrations: %v", err)
	}
}

// Test ClickHouse migration down operations
func TestClickHouseMigrateDown(t *testing.T) {
	testConfig := testdb.GetTestConfig()

	// Create test configuration for ClickHouse
	config := internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
		LogLevel:           "error",
	}

	// Wait for database to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get ClickHouse connection string and test connection
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	require.NoError(t, err, "failed to open ClickHouse connection")
	defer d.Close()

	// Test connection by creating SQL connection (without migration parameters)
	testConnectionString2 := config.GetClickHouseConnectionString(false)
	chTestDB2, err := sql.Open("clickhouse", testConnectionString2)
	require.NoError(t, err, "failed to create ClickHouse test connection")
	defer chTestDB2.Close()

	// Wait for connection
	for {
		if err := chTestDB2.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("ClickHouse not ready within timeout")
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://../db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	require.NoError(t, err, "failed to create ClickHouse migrate instance")
	defer m.Close()

	// First apply all migrations
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply ClickHouse migrations")
	}

	// Create a new connection for queries (without migration-specific parameters)
	queryConnectionString := config.GetClickHouseConnectionString(false)
	chDB, err := sql.Open("clickhouse", queryConnectionString)
	require.NoError(t, err, "failed to create ClickHouse SQL connection")
	defer chDB.Close()

	// Verify tables exist before down migration
	query := "SELECT count() FROM system.tables WHERE database = ? AND name = ?"
	var count int
	err = chDB.QueryRow(query, config.ClickHouseDatabase, "accounts_archive").Scan(&count)
	require.NoError(t, err, "failed to query accounts_archive table existence")
	assert.Equal(t, 1, count, "accounts_archive table should exist before down migration")

	// Test migration down
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to rollback ClickHouse migrations")
	}

	// Verify tables no longer exist
	err = chDB.QueryRow(query, config.ClickHouseDatabase, "accounts_archive").Scan(&count)
	require.NoError(t, err, "failed to query accounts_archive table existence after down")
	assert.Equal(t, 0, count, "accounts_archive table should not exist after down migration")

	err = chDB.QueryRow(query, config.ClickHouseDatabase, "storage_archive").Scan(&count)
	require.NoError(t, err, "failed to query storage_archive table existence after down")
	assert.Equal(t, 0, count, "storage_archive table should not exist after down migration")

	// Verify version is back to nil (no migrations applied)
	_, _, err = m.Version()
	assert.Equal(t, migrate.ErrNilVersion, err, "should have no version after complete down migration")
}

// Test migration status checking for ClickHouse
func TestClickHouseMigrateStatus(t *testing.T) {
	testConfig := testdb.GetTestConfig()

	// Create test configuration for ClickHouse
	config := internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
		LogLevel:           "error",
	}

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	require.NoError(t, err, "failed to open ClickHouse connection")
	defer d.Close()

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://../db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	require.NoError(t, err, "failed to create ClickHouse migrate instance")
	defer m.Close()

	// Test status before any migrations
	version, dirty, err := m.Version()
	assert.Equal(t, migrate.ErrNilVersion, err, "should have no version before ClickHouse migrations")

	// Apply migrations
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply ClickHouse migrations")
	}

	// Test status after migrations
	version, dirty, err = m.Version()
	require.NoError(t, err, "failed to get ClickHouse migration version")
	assert.False(t, dirty, "ClickHouse migration should not be in dirty state")
	assert.Greater(t, version, uint(0), "ClickHouse migration version should be greater than 0")

	// Clean up
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		t.Logf("Warning: failed to clean up ClickHouse migrations: %v", err)
	}
}

// Test migration idempotency (running same migration twice) for ClickHouse
func TestClickHouseMigrateIdempotency(t *testing.T) {
	testConfig := testdb.GetTestConfig()

	// Create test configuration for ClickHouse
	config := internal.Config{
		ClickHouseHost:     testConfig.ClickHouse.Host,
		ClickHousePort:     testConfig.ClickHouse.Port,
		ClickHouseUser:     testConfig.ClickHouse.User,
		ClickHousePassword: testConfig.ClickHouse.Password,
		ClickHouseDatabase: testConfig.ClickHouse.Database,
		ClickHouseMaxConns: 10,
		ClickHouseMinConns: 2,
		RPCURLS:            []string{"http://localhost:8545"},
		Environment:        "test",
		LogLevel:           "error",
	}

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	require.NoError(t, err, "failed to open ClickHouse connection")
	defer d.Close()

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://../db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	require.NoError(t, err, "failed to create ClickHouse migrate instance")
	defer m.Close()

	// Apply migrations first time
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "failed to apply ClickHouse migrations first time")
	}

	// Get version after first migration
	version1, dirty1, err := m.Version()
	require.NoError(t, err, "failed to get ClickHouse migration version after first migration")
	assert.False(t, dirty1, "ClickHouse migration should not be in dirty state after first migration")

	// Apply migrations second time (should be idempotent)
	err = m.Up()
	assert.Equal(t, migrate.ErrNoChange, err, "second ClickHouse migration should return ErrNoChange")

	// Get version after second migration
	version2, dirty2, err := m.Version()
	require.NoError(t, err, "failed to get ClickHouse migration version after second migration")
	assert.False(t, dirty2, "ClickHouse migration should not be in dirty state after second migration")
	assert.Equal(t, version1, version2, "ClickHouse version should be the same after idempotent migration")

	// Verify tables still exist and are functional
	queryConnectionString := config.GetClickHouseConnectionString(false)
	chDB, err := sql.Open("clickhouse", queryConnectionString)
	require.NoError(t, err, "failed to create ClickHouse SQL connection")
	defer chDB.Close()

	query := "SELECT count() FROM system.tables WHERE database = ? AND name = ?"
	var count int
	err = chDB.QueryRow(query, config.ClickHouseDatabase, "accounts_archive").Scan(&count)
	require.NoError(t, err, "failed to query accounts_archive table existence after idempotent migration")
	assert.Equal(t, 1, count, "accounts_archive table should still exist after idempotent migration")

	// Clean up
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		t.Logf("Warning: failed to clean up ClickHouse migrations: %v", err)
	}
}
