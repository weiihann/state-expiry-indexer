package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

// BuildConnectionString creates a PostgreSQL connection string from config
// Deprecated: Use config.GetDatabaseConnectionString() instead
func BuildConnectionString(config internal.Config) string {
	return config.GetDatabaseConnectionString()
}

// Connect creates a new pgxpool connection for application use
func Connect(ctx context.Context, config internal.Config) (*pgxpool.Pool, error) {
	log := logger.GetLogger("database")
	connStr := config.GetDatabaseConnectionString()

	dbConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("could not parse database config: %w", err)
	}

	// Set connection pool settings from config
	dbConfig.MaxConns = int32(config.DBMaxConns)
	dbConfig.MinConns = int32(config.DBMinConns)

	db, err := pgxpool.ConnectConfig(ctx, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("could not ping database: %w", err)
	}

	log.Info("Database connection established",
		"host", config.DBHost,
		"port", config.DBPort,
		"database", config.DBName,
		"max_conns", dbConfig.MaxConns,
		"min_conns", dbConfig.MinConns,
		"environment", config.Environment)
	return db, nil
}

// ConnectSQL creates a standard database/sql connection for golang-migrate
func ConnectSQL(config internal.Config) (*sql.DB, error) {
	connStr := config.GetDatabaseConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("could not ping database: %w", err)
	}

	return db, nil
}
