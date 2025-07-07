package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
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

// ConnectClickHouse creates a ClickHouse connection for archive mode
func ConnectClickHouse(ctx context.Context, config internal.Config) (clickhouse.Conn, error) {
	log := logger.GetLogger("clickhouse-database")

	// Create ClickHouse connection options
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", config.ClickHouseHost, config.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: config.ClickHouseDatabase,
			Username: config.ClickHouseUser,
			Password: config.ClickHousePassword,
		},
		// Optional settings for better performance
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 30000, // 30 seconds in milliseconds
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    5,
		MaxIdleConns:    3,
		ConnMaxLifetime: 300000, // 5 minutes in milliseconds
	}

	// Create the connection
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("could not create ClickHouse connection: %w", err)
	}

	// Test the connection
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("could not ping ClickHouse database: %w", err)
	}

	log.Info("ClickHouse connection established",
		"host", config.ClickHouseHost,
		"port", config.ClickHousePort,
		"database", config.ClickHouseDatabase,
		"user", config.ClickHouseUser,
		"environment", config.Environment)

	return conn, nil
}

// ConnectClickHouseSQL creates a standard database/sql ClickHouse connection for golang-migrate
func ConnectClickHouseSQL(config internal.Config) (*sql.DB, error) {
	log := logger.GetLogger("clickhouse-migration")
	connStr := config.GetClickHouseConnectionString()

	// Open ClickHouse connection using database/sql interface
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not open ClickHouse connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("could not ping ClickHouse database: %w", err)
	}

	log.Info("ClickHouse migration connection established",
		"connection_string", connStr,
		"host", config.ClickHouseHost,
		"port", config.ClickHousePort,
		"database", config.ClickHouseDatabase)

	return db, nil
}
